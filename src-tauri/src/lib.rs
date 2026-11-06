use chrono::Local;
use grammers_client::types::{Chat, LoginToken, PasswordToken};
use grammers_client::{Client, Config, SignInError, Update};
use grammers_session::Session;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tauri::{AppHandle, Emitter, Manager, State};
use tokio::net::TcpStream;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::timeout;

#[derive(Serialize, Deserialize, Clone)]
pub struct GhostSettings {
    pub anti_delete: bool,
    pub no_read: bool,
    pub hide_online: bool,
    pub save_edits: bool,
    pub media_safe: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DialogItem {
    pub id: i64,
    pub title: String,
    pub last_message: String,
    pub unread: i32,
    pub is_channel: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct EditRecord {
    pub old_text: String,
    pub edit_time: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ChatMessage {
    pub id: i64,
    pub chat_id: i64,
    pub sender: String,
    pub text: String,
    pub is_outgoing: bool,
    pub is_deleted: bool,
    pub has_media: bool,
    pub time: String,
    pub edits: Vec<EditRecord>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ProxyItem {
    pub proxy_type: String,
    pub url: String,
    pub host: String,
    pub port: u16,
    pub ping_ms: u32,
    pub region: String,
}

pub struct TgAuthState {
    pub client: Option<Client>,
    pub login_token: Option<LoginToken>,
    pub password_token: Option<PasswordToken>,
    pub session_path: PathBuf,
    pub media_dir: PathBuf,
    pub api_id: i32,
    pub api_hash: String,
    pub listener_running: bool,
    pub peers: HashMap<i64, Chat>,
}

pub struct AppState {
    pub db: Arc<Mutex<Connection>>,
    pub settings: Arc<Mutex<GhostSettings>>,
    pub tg: Arc<AsyncMutex<TgAuthState>>,
    pub active_chat: Arc<Mutex<i64>>,
}

fn init_db(conn: &Connection) {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS chats (id INTEGER PRIMARY KEY, title TEXT NOT NULL, last_msg TEXT NOT NULL, unread INTEGER DEFAULT 0, is_channel INTEGER DEFAULT 0)",
        [],
    ).unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS messages (id INTEGER NOT NULL, chat_id INTEGER NOT NULL, sender TEXT NOT NULL, text TEXT NOT NULL, is_outgoing INTEGER NOT NULL, is_deleted INTEGER DEFAULT 0, has_media INTEGER DEFAULT 0, time TEXT NOT NULL, PRIMARY KEY (chat_id, id))",
        [],
    ).unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS edits (id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL, msg_id INTEGER NOT NULL, old_text TEXT NOT NULL, edit_time TEXT NOT NULL)",
        [],
    ).unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS app_config (key TEXT PRIMARY KEY, val TEXT NOT NULL)",
        [],
    ).unwrap();

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM chats", [], |r| r.get(0)).unwrap_or(0);
    if count == 0 {
        conn.execute(
            "INSERT INTO chats (id, title, last_msg, unread, is_channel) VALUES (1001, 'Дмитрий (Клиент)', 'Медиа-сейф активен', 1, 0)",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO messages (id, chat_id, sender, text, is_outgoing, is_deleted, has_media, time) VALUES (101, 1001, 'Дмитрий', 'Скинул файл в кэш.', 0, 0, 1, '12:10')",
            [],
        ).unwrap();
    }
}

fn fetch_dialogs_from_db(conn: &Connection) -> Vec<DialogItem> {
    let mut stmt = conn.prepare("SELECT id, title, last_msg, unread, is_channel FROM chats ORDER BY id ASC").unwrap();
    let rows = stmt.query_map([], |r| {
        Ok(DialogItem { id: r.get(0)?, title: r.get(1)?, last_message: r.get(2)?, unread: r.get(3)?, is_channel: r.get::<_, i64>(4)? != 0 })
    }).unwrap();
    rows.flatten().collect()
}

fn fetch_chat_messages(conn: &Connection, chat_id: i64) -> Vec<ChatMessage> {
    let mut stmt = conn.prepare("SELECT id, chat_id, sender, text, is_outgoing, is_deleted, has_media, time FROM messages WHERE chat_id = ?1 ORDER BY id ASC").unwrap();
    let rows = stmt.query_map(params![chat_id], |row| {
        Ok(ChatMessage { id: row.get(0)?, chat_id: row.get(1)?, sender: row.get(2)?, text: row.get(3)?, is_outgoing: row.get::<_, i64>(4)? != 0, is_deleted: row.get::<_, i64>(5)? != 0, has_media: row.get::<_, i64>(6)? != 0, time: row.get(7)?, edits: Vec::new() })
    }).unwrap();

    let mut result = Vec::new();
    for msg in rows.flatten() {
        let mut m = msg;
        let mut edit_stmt = conn.prepare("SELECT old_text, edit_time FROM edits WHERE chat_id = ?1 AND msg_id = ?2 ORDER BY id ASC").unwrap();
        let edits_iter = edit_stmt.query_map(params![m.chat_id, m.id], |r| {
            Ok(EditRecord { old_text: r.get(0)?, edit_time: r.get(1)? })
        }).unwrap();
        m.edits = edits_iter.flatten().collect();
        result.push(m);
    }
    result
}

fn spawn_update_interceptor(
    client: Client,
    db: Arc<Mutex<Connection>>,
    settings: Arc<Mutex<GhostSettings>>,
    tg_state: Arc<AsyncMutex<TgAuthState>>,
    app_handle: AppHandle,
) {
    tauri::async_runtime::spawn(async move {
        loop {
            match client.next_update().await {
                Ok(Some(update)) => {
                    if let Update::NewMessage(message) = update {
                        let chat = message.chat();
                        let chat_id = chat.id();
                        let chat_title = chat.name().to_string();
                        let msg_id = message.id() as i64;
                        let text = message.text().to_string();
                        let has_media = message.media().is_some();
                        if text.is_empty() && !has_media { continue; }
                        let display_text = if text.is_empty() { "[Медиафайл]".to_string() } else { text };
                        let sender = message.sender().map(|s| s.name().to_string()).unwrap_or_else(|| chat_title.clone());
                        let is_out = if message.outgoing() { 1 } else { 0 };
                        let now = Local::now().format("%H:%M").to_string();

                        let media_safe_on = settings.lock().unwrap().media_safe;
                        if media_safe_on && has_media {
                            let media_dir = { tg_state.lock().await.media_dir.clone() };
                            let _ = std::fs::create_dir_all(&media_dir);
                            let file_path = media_dir.join(format!("media_{}_{}.dat", chat_id, msg_id));
                            if let Ok(file_download) = message.download().await {
                                let _ = std::fs::write(&file_path, &file_download);
                            }
                        }

                        {
                            let mut tg = tg_state.lock().await;
                            tg.peers.insert(chat_id, chat);
                        }

                        {
                            let conn = db.lock().unwrap();
                            let _ = conn.execute(
                                "INSERT OR REPLACE INTO chats (id, title, last_msg, unread, is_channel) VALUES (?1, ?2, ?3, COALESCE((SELECT unread FROM chats WHERE id = ?1), 0) + 1, 0)",
                                params![chat_id, chat_title, display_text],
                            );
                            let _ = conn.execute(
                                "INSERT OR REPLACE INTO messages (id, chat_id, sender, text, is_outgoing, is_deleted, has_media, time) VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7)",
                                params![msg_id, chat_id, sender, display_text, is_out, if has_media { 1 } else { 0 }, now],
                            );
                        }
                        let _ = app_handle.emit("db-updated", ());
                    }
                }
                Ok(None) => break,
                Err(_) => { tokio::time::sleep(std::time::Duration::from_secs(3)).await; }
            }
        }
    });
}

#[tauri::command]
async fn sync_dialogs(state: State<'_, AppState>) -> Result<Vec<DialogItem>, String> {
    let maybe_client = { state.tg.lock().await.client.clone() };
    if let Some(client) = maybe_client {
        let mut dialogs = client.iter_dialogs();
        let mut count = 0;
        while let Some(dialog) = dialogs.next().await.map_err(|e| e.to_string())? {
            let chat = dialog.chat().clone();
            let chat_id = chat.id();
            let title = chat.name().to_string();
            let is_channel = matches!(chat, Chat::Channel(_));
            let last_msg = dialog.last_message.as_ref().map(|m| m.text().to_string()).unwrap_or_else(|| "Медиафайл".to_string());

            {
                let mut tg = state.tg.lock().await;
                tg.peers.insert(chat_id, chat);
            }
            {
                let conn = state.db.lock().unwrap();
                let _ = conn.execute(
                    "INSERT OR REPLACE INTO chats (id, title, last_msg, unread, is_channel) VALUES (?1, ?2, ?3, 0, ?4)",
                    params![chat_id, title, last_msg, if is_channel { 1 } else { 0 }],
                );
            }
            count += 1;
            if count >= 35 { break; }
        }
    }
    let conn = state.db.lock().unwrap();
    Ok(fetch_dialogs_from_db(&conn))
}

#[tauri::command]
async fn select_chat(chat_id: i64, state: State<'_, AppState>) -> Result<Vec<ChatMessage>, String> {
    { *state.active_chat.lock().unwrap() = chat_id; }
    {
        let conn = state.db.lock().unwrap();
        let _ = conn.execute("UPDATE chats SET unread = 0 WHERE id = ?1", params![chat_id]);
    }
    let conn = state.db.lock().unwrap();
    Ok(fetch_chat_messages(&conn, chat_id))
}

#[tauri::command]
async fn get_fast_proxies() -> Result<Vec<ProxyItem>, String> {
    let mut checked = Vec::new();
    let sources = vec![
        ("mtproto", "RU", "https://raw.githubusercontent.com/kort0881/telegram-proxy-collector/main/proxy_ru.txt"),
    ];
    if let Ok(http) = reqwest::Client::builder().timeout(std::time::Duration::from_secs(3)).build() {
        for (ptype, region, url_src) in sources {
            if let Ok(resp) = http.get(url_src).send().await {
                if let Ok(text) = resp.text().await {
                    for line in text.lines().take(8) {
                        let line = line.trim();
                        if line.starts_with("tg://") {
                            if let Some((host, port)) = parse_host_port(line) {
                                let start = Instant::now();
                                let addr = format!("{}:{}", host, port);
                                if timeout(std::time::Duration::from_millis(1000), TcpStream::connect(&addr)).await.is_ok() {
                                    checked.push(ProxyItem { proxy_type: ptype.to_string(), url: line.to_string(), host, port, ping_ms: start.elapsed().as_millis() as u32, region: region.to_string() });
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    checked.sort_by_key(|p| p.ping_ms);
    Ok(checked)
}

fn parse_host_port(tg_url: &str) -> Option<(String, u16)> {
    let url = url::Url::parse(tg_url).ok()?;
    let pairs: HashMap<_, _> = url.query_pairs().into_owned().collect();
    Some((pairs.get("server")?.clone(), pairs.get("port")?.parse::<u16>().ok()?))
}

#[tauri::command]
async fn check_auth_status(state: State<'_, AppState>, app_handle: AppHandle) -> Result<bool, String> {
    let mut tg = state.tg.lock().await;
    if let Some(client) = &tg.client {
        return client.is_authorized().await.map_err(|e| e.to_string());
    }
    let (saved_id, saved_hash) = {
        let conn = state.db.lock().unwrap();
        (conn.query_row("SELECT val FROM app_config WHERE key = 'api_id'", [], |r| r.get(0)).ok(),
         conn.query_row("SELECT val FROM app_config WHERE key = 'api_hash'", [], |r| r.get(0)).ok())
    };
    if let (Some(id_str), Some(hash_str)) = (saved_id, saved_hash) {
        if let Ok(api_id) = id_str.parse::<i32>() {
            if tg.session_path.exists() {
                let session = Session::load_file_or_create(&tg.session_path).map_err(|e| e.to_string())?;
                let client = Client::connect(Config { session, api_id, api_hash: hash_str.clone(), params: Default::default() }).await.map_err(|e| e.to_string())?;
                if client.is_authorized().await.unwrap_or(false) {
                    if !tg.listener_running {
                        tg.listener_running = true;
                        spawn_update_interceptor(client.clone(), state.db.clone(), state.settings.clone(), state.tg.clone(), app_handle);
                    }
                    tg.client = Some(client);
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

#[tauri::command]
async fn auth_request_code(api_id: i32, api_hash: String, phone: String, state: State<'_, AppState>) -> Result<String, String> {
    let mut tg = state.tg.lock().await;
    tg.api_id = api_id;
    tg.api_hash = api_hash.clone();
    {
        let conn = state.db.lock().unwrap();
        let _ = conn.execute("INSERT OR REPLACE INTO app_config (key, val) VALUES ('api_id', ?1)", params![api_id.to_string()]);
        let _ = conn.execute("INSERT OR REPLACE INTO app_config (key, val) VALUES ('api_hash', ?1)", params![api_hash]);
    }
    let session = Session::load_file_or_create(&tg.session_path).map_err(|e| e.to_string())?;
    let client = Client::connect(Config { session, api_id, api_hash: tg.api_hash.clone(), params: Default::default() }).await.map_err(|e| e.to_string())?;
    let token = client.request_login_code(&phone).await.map_err(|e| e.to_string())?;
    tg.client = Some(client);
    tg.login_token = Some(token);
    Ok("CODE_SENT".to_string())
}

#[tauri::command]
async fn auth_verify_code(code: String, state: State<'_, AppState>, app_handle: AppHandle) -> Result<String, String> {
    let mut tg = state.tg.lock().await;
    let client = tg.client.as_ref().ok_or("Нет клиента")?.clone();
    let token = tg.login_token.take().ok_or("Нет токена")?;
    match client.sign_in(&token, &code).await {
        Ok(_) => {
            client.session().save_to_file(&tg.session_path).ok();
            if !tg.listener_running {
                tg.listener_running = true;
                spawn_update_interceptor(client, state.db.clone(), state.settings.clone(), state.tg.clone(), app_handle);
            }
            Ok("SUCCESS".to_string())
        }
        Err(SignInError::PasswordRequired(pwd_token)) => {
            tg.password_token = Some(pwd_token);
            Ok("2FA_REQUIRED".to_string())
        }
        Err(e) => Err(e.to_string()),
    }
}

#[tauri::command]
async fn auth_verify_2fa(password: String, state: State<'_, AppState>, app_handle: AppHandle) -> Result<String, String> {
    let mut tg = state.tg.lock().await;
    let client = tg.client.as_ref().ok_or("Нет клиента")?.clone();
    let pwd_token = tg.password_token.take().ok_or("Нет токена 2FA")?;
    client.check_password(pwd_token, password.as_bytes()).await.map_err(|e| e.to_string())?;
    client.session().save_to_file(&tg.session_path).ok();
    if !tg.listener_running {
        tg.listener_running = true;
        spawn_update_interceptor(client, state.db.clone(), state.settings.clone(), state.tg.clone(), app_handle);
    }
    Ok("SUCCESS".to_string())
}

#[tauri::command]
fn get_initial_data(state: State<AppState>) -> (GhostSettings, Vec<DialogItem>, i64, Vec<ChatMessage>) {
    let settings = state.settings.lock().unwrap().clone();
    let active_id = *state.active_chat.lock().unwrap();
    let conn = state.db.lock().unwrap();
    (settings, fetch_dialogs_from_db(&conn), active_id, fetch_chat_messages(&conn, active_id))
}

#[tauri::command]
fn toggle_setting(key: String, state: State<AppState>) -> GhostSettings {
    let mut s = state.settings.lock().unwrap();
    match key.as_str() {
        "anti_delete" => s.anti_delete = !s.anti_delete,
        "no_read" => s.no_read = !s.no_read,
        "hide_online" => s.hide_online = !s.hide_online,
        "save_edits" => s.save_edits = !s.save_edits,
        "media_safe" => s.media_safe = !s.media_safe,
        _ => {}
    }
    s.clone()
}

#[tauri::command]
async fn send_message(text: String, state: State<'_, AppState>) -> Result<Vec<ChatMessage>, String> {
    let now = Local::now().format("%H:%M").to_string();
    let chat_id = *state.active_chat.lock().unwrap();
    let (maybe_client, maybe_peer) = {
        let tg = state.tg.lock().await;
        (tg.client.clone(), tg.peers.get(&chat_id).cloned())
    };
    let mut sent_msg_id = Local::now().timestamp_millis();
    if let Some(client) = maybe_client {
        if let Some(peer) = maybe_peer {
            if let Ok(sent) = client.send_message(&peer, text.as_str()).await {
                sent_msg_id = sent.id() as i64;
            }
        }
    }
    let conn = state.db.lock().unwrap();
    conn.execute("INSERT OR REPLACE INTO messages (id, chat_id, sender, text, is_outgoing, is_deleted, has_media, time) VALUES (?1, ?2, 'Вы', ?3, 1, 0, 0, ?4)", params![sent_msg_id, chat_id, text, now]).unwrap();
    Ok(fetch_chat_messages(&conn, chat_id))
}

#[tauri::command]
fn intercept_delete(msg_id: i64, state: State<AppState>) -> Vec<ChatMessage> {
    let chat_id = *state.active_chat.lock().unwrap();
    let conn = state.db.lock().unwrap();
    let s = state.settings.lock().unwrap();
    if s.anti_delete {
        conn.execute("UPDATE messages SET is_deleted = 1 WHERE chat_id = ?1 AND id = ?2", params![chat_id, msg_id]).unwrap();
    } else {
        conn.execute("DELETE FROM messages WHERE chat_id = ?1 AND id = ?2", params![chat_id, msg_id]).unwrap();
    }
    fetch_chat_messages(&conn, chat_id)
}

#[tauri::command]
fn intercept_edit(msg_id: i64, new_text: String, state: State<AppState>) -> Vec<ChatMessage> {
    let chat_id = *state.active_chat.lock().unwrap();
    let conn = state.db.lock().unwrap();
    let s = state.settings.lock().unwrap();
    let now = Local::now().format("%H:%M:%S").to_string();
    if s.save_edits {
        if let Ok(old_text) = conn.query_row::<String, _, _>("SELECT text FROM messages WHERE chat_id = ?1 AND id = ?2", params![chat_id, msg_id], |r| r.get(0)) {
            if old_text != new_text {
                conn.execute("INSERT INTO edits (chat_id, msg_id, old_text, edit_time) VALUES (?1, ?2, ?3, ?4)", params![chat_id, msg_id, old_text, now]).unwrap();
            }
        }
    }
    conn.execute("UPDATE messages SET text = ?1 WHERE chat_id = ?2 AND id = ?3", params![new_text, chat_id, msg_id]).unwrap();
    fetch_chat_messages(&conn, chat_id)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .setup(|app| {
            let app_dir = app.path().app_data_dir().unwrap_or_else(|_| std::env::temp_dir());
            std::fs::create_dir_all(&app_dir).ok();
            let conn = Connection::open(app_dir.join("vortex_v5.db")).unwrap();
            init_db(&conn);
            app.manage(AppState {
                db: Arc::new(Mutex::new(conn)),
                settings: Arc::new(Mutex::new(GhostSettings { anti_delete: true, no_read: true, hide_online: true, save_edits: true, media_safe: true })),
                tg: Arc::new(AsyncMutex::new(TgAuthState { client: None, login_token: None, password_token: None, session_path: app_dir.join("v.session"), media_dir: app_dir.join("media"), api_id: 0, api_hash: String::new(), listener_running: false, peers: HashMap::new() })),
                active_chat: Arc::new(Mutex::new(1001)),
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            check_auth_status, auth_request_code, auth_verify_code, auth_verify_2fa,
            sync_dialogs, select_chat, get_fast_proxies, get_initial_data,
            toggle_setting, send_message, intercept_delete, intercept_edit
        ])
        .run(tauri::generate_context!())
        .expect("error running");
}
