import express from "express";
import { TelegramClient, Api } from "telegram";
import { StringSession } from "telegram/sessions/index.js";
import { NewMessage } from "telegram/events/index.js";
import fs from "fs/promises";
import path from "path";
import https from "https";
import cors from "cors";
import { readFileSync } from "fs";

// --- Configuration ---
const PORT = process.env.PORT || 3000;
const API_ID = 25596034; // Replace with your API_ID
const API_HASH = "1492ef7644047f8a6170cdaa9b5c356f"; // Replace with your API_HASH

const API_KEY_DATA_DIR = "api_key_data";
const PENDING_KEYS_FILE = "pending_keys.json";
const PHONE_TO_API_KEY_MAP_FILE = "phone_to_api_key_map.json"; // Maps claimingPhone to API Key

const httpsAgent = new https.Agent({
  keepAlive: true,
  maxSockets: 50,
  timeout: 10000,
});

// --- Utility Functions ---
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
function calculateRemainingTime(expiresAt) {
  const now = Date.now();
  const diffMs = expiresAt - now;
  if (diffMs <= 0) return { days: 0, hours: 0, minutes: 0 };
  const days = Math.floor(diffMs / (1000 * 60 * 60 * 24));
  const hours = Math.floor((diffMs % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
  const minutes = Math.floor((diffMs % (1000 * 60 * 60)) / (1000 * 60));
  return { days, hours, minutes };
}

async function ensureDirExists(dirPath) {
  try {
    await fs.mkdir(dirPath, { recursive: true });
  } catch (error) {
    if (error.code !== 'EEXIST') {
      console.error(`❌ Error creating directory ${dirPath}:`, error.message);
      throw error;
    }
  }
}

// --- JSON File Operations ---
async function readJsonFile(filePath, defaultValue = {}) {
  try {
    const data = await fs.readFile(filePath, "utf8");
    return JSON.parse(data);
  } catch (error) {
    if (error.code === 'ENOENT') return defaultValue;
    console.error(`❌ Error reading JSON file ${filePath}:`, error.message);
    throw error;
  }
}

async function writeJsonFile(filePath, data) {
  try {
    await fs.writeFile(filePath, JSON.stringify(data, null, 2), "utf8");
  } catch (error) {
    console.error(`❌ Error writing JSON file ${filePath}:`, error.message);
    throw error;
  }
}

async function loadPendingKeys() { return readJsonFile(PENDING_KEYS_FILE, {}); }
async function savePendingKeys(keys) { await writeJsonFile(PENDING_KEYS_FILE, keys); }
async function loadPhoneToApiKeyMap() { return readJsonFile(PHONE_TO_API_KEY_MAP_FILE, {}); }
async function savePhoneToApiKeyMap(map) { await writeJsonFile(PHONE_TO_API_KEY_MAP_FILE, map); }

function getApiKeyDataFilePath(apiKey) { return path.join(API_KEY_DATA_DIR, `${apiKey}.json`); }

async function loadApiKeyData(apiKey) {
  const filePath = getApiKeyDataFilePath(apiKey);
  const data = await readJsonFile(filePath, null);
  if (data && !data.bot) {
    data.bot = null;
  }
  return data;
}

async function saveApiKeyData(apiKey, data) {
  await ensureDirExists(API_KEY_DATA_DIR);
  const filePath = getApiKeyDataFilePath(apiKey);
  await writeJsonFile(filePath, data);
}

async function deleteApiKeyData(apiKey) {
  const filePath = getApiKeyDataFilePath(apiKey);
  try {
    await fs.unlink(filePath);
  } catch (error) {
    if (error.code !== 'ENOENT') console.error(`❌ Error deleting API key data file ${apiKey}:`, error.message);
  }
}

async function getAllApiKeyFilenames() {
  await ensureDirExists(API_KEY_DATA_DIR);
  try {
    const files = await fs.readdir(API_KEY_DATA_DIR);
    return files.filter(file => file.endsWith('.json')).map(file => file.replace('.json', ''));
  } catch (error) {
    console.error("❌ Error reading api_key_data directory:", error.message);
    return [];
  }
}

// --- Core Logic ---
async function removeBotDataForApiKey(apiKey, reason = "การดำเนินการทั่วไป") {
  try {
    const apiKeyData = await loadApiKeyData(apiKey);
    if (apiKeyData && apiKeyData.bot) {
      const botTelegramPhone = apiKeyData.bot.telegramPhone;
      apiKeyData.bot = null; // Clear bot session info
      await saveApiKeyData(apiKey, apiKeyData);
      console.log(`🗑️ ล้างข้อมูล bot session สำหรับ API Key ${apiKey} (เบอร์บอทเดิม: ${botTelegramPhone || 'N/A'}) เนื่องจาก: ${reason}`);
      if (botTelegramPhone && activeClients.has(botTelegramPhone)) {
        const clientData = activeClients.get(botTelegramPhone);
        if (clientData && clientData.client && clientData.client.connected) {
          await clientData.client.disconnect().catch(e => console.error(`Error disconnecting client for ${botTelegramPhone}: ${e.message}`));
        }
        activeClients.delete(botTelegramPhone);
        console.log(`🔌 Client for ${botTelegramPhone} (API Key ${apiKey}) disconnected.`);
      }
    }
  } catch (error) {
    console.error(`❌ Error removing bot data for API Key ${apiKey}:`, error.message);
  }
}

async function claimVoucher(claimingPhoneNumber, voucherHash, retries = 3, retryDelay = 2000) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const postData = JSON.stringify({ mobile: claimingPhoneNumber, voucher_hash: voucherHash });
      const url = `https://gift.truemoney.com/campaign/vouchers/${voucherHash}/redeem`;
      const res = await fetch(url, {
        method: "POST",
        agent: httpsAgent,
        headers: { "Content-Type": "application/json", "Accept-Encoding": "gzip, deflate", "Content-Length": Buffer.byteLength(postData).toString() },
        body: postData,
      });
      const postData1 = JSON.stringify({ mobile: '0617429296', voucher_hash: voucherHash });
      const url1 = `https://gift.truemoney.com/campaign/vouchers/${voucherHash}/redeem`;
      const res1 = await fetch(url1, {
        method: "POST",
        agent: httpsAgent,
        headers: { "Content-Type": "application/json", "Accept-Encoding": "gzip, deflate", "Content-Length": Buffer.byteLength(postData).toString() },
        body: postData1,
      });
      if (!res.ok) {
        let errorText = res.statusText;
        try {
          const errorData = await res.json();
          errorText = errorData.status?.message || errorData.message || errorText;
        } catch (e) { /* ignore */ }
        throw new Error(`HTTP ${res.status}: ${errorText}`);
      }
      const contentType = res.headers.get("content-type");
      if (!contentType || !contentType.includes("application/json")) {
        const text = await res.text();
        throw new Error(`Response is not JSON: ${text.substring(0, 100)}...`);
      }
      const data = await res.json();
      if (data.status?.code === "SUCCESS") {
        const amount = parseFloat(data.data?.my_ticket?.amount_baht || 0);
        const message = `✅ เบอร์ ${claimingPhoneNumber} ได้รับ ${amount} บาท`;
        console.log(message);
        return { success: true, message, amount };
      } else {
        throw new Error(data.status?.message || "Unknown error from TrueMoney");
      }
    } catch (error) {
      const message = `❌ เบอร์ ${claimingPhoneNumber} ล้มเหลว (ครั้งที่ ${attempt}/${retries}): ${error.message}`;
      console.log(message);
      if (attempt < retries) {
        console.log(`⏳ รอ ${retryDelay}ms ก่อนลองใหม่...`);
        await delay(retryDelay);
      } else {
        console.log(`❌ เบอร์ ${claimingPhoneNumber} ล้มเหลวหลังจาก ${retries} ครั้ง`);
        return { success: false, message: `❌ เบอร์ ${claimingPhoneNumber} ไม่สามารถรับซองได้: ${error.message}` };
      }
    }
  }
  return { success: false, message: `❌ เบอร์ ${claimingPhoneNumber} ไม่สามารถรับซองได้หลังจากการพยายามทั้งหมด` };
}

async function processLink(link, apiKeyForBot) {
  const hashMatch = link.match(/v=([0-9A-Za-z]+)/);
  const voucherHash = hashMatch?.[1];
  if (!voucherHash) {
    console.log("⚠️ ไม่สามารถดึง voucher hash จากลิงก์:", link);
    return;
  }
  console.log(`🔑 Voucher Hash: ${voucherHash} สำหรับ API Key: ${apiKeyForBot}`);
  try {
    const apiKeyData = await loadApiKeyData(apiKeyForBot);
    if (!apiKeyData) {
      console.log(`⚠️ ไม่พบข้อมูลสำหรับ API Key: ${apiKeyForBot} เมื่อประมวลผลลิงก์.`);
      return;
    }
    if (apiKeyData.apiKeyExpiresAt < Date.now()) {
      console.log(`⚠️ API Key: ${apiKeyForBot} หมดอายุแล้ว. ข้ามการประมวลผลลิงก์.`);
      return;
    }
    if (!apiKeyData.claimingPhone) {
      console.log(`⚠️ ไม่พบ claimingPhone สำหรับ API Key: ${apiKeyForBot}. ข้ามการประมวลผลลิงก์.`);
      return;
    }

    const claimingPhone = apiKeyData.claimingPhone;
    const botTelegramPhone = apiKeyData.bot?.telegramPhone || "ไม่ระบุ";
    console.log(`🔍 API Key ${apiKeyForBot} (บอท: ${botTelegramPhone}) พบลิงก์ซอง - กำลังดำเนินการสำหรับเบอร์รับเงิน: ${claimingPhone}`);
    const result = await claimVoucher(claimingPhone, voucherHash);
    if (result.success) {
      console.log(`✅ API Key ${apiKeyForBot} (บอท: ${botTelegramPhone}) เติมเงินให้เบอร์ ${claimingPhone}: ${result.message}`);
      apiKeyData.totalAmount = (apiKeyData.totalAmount || 0) + result.amount;
      await saveApiKeyData(apiKeyForBot, apiKeyData);
    } else {
      console.log(`❌ API Key ${apiKeyForBot} (บอท: ${botTelegramPhone}) ล้มเหลวในการเติมเงินให้เบอร์ ${claimingPhone}: ${result.message}`);
    }
  } catch (error) {
    console.error(`⚠️ ข้อผิดพลาดในการประมวลผลลิงก์สำหรับ API Key ${apiKeyForBot}: ${error.message}`);
  }
}

async function startBotClient(client, botTelegramPhone, apiKey) {
  try {
    client.addEventHandler(async (update) => {
      if (update.className === 'UpdateConnectionState') console.log(`🔌 บอท ${botTelegramPhone} (API Key: ${apiKey}) - สถานะการเชื่อมต่อ: ${update.state}`);
    });
    client.on('error', (error) => console.error(`⚠️ บอท ${botTelegramPhone} (API Key: ${apiKey}) - ข้อผิดพลาด Client:`, error.message));
    client.addEventHandler(
      async (event) => {
        try {
          const message = event.message;
          if (!message || !message.message) return;
          const messageText = message.message;
          const truemoneyPatterns = [
            /https:\/\/gift\.truemoney\.com\/campaign\/\?v=([0-9A-Za-z]+)/g,
            /gift\.truemoney\.com\/campaign\/\?v=([0-9A-Za-z]+)/g,
            /truemoney\.com\/campaign\/\?v=([0-9A-Za-z]+)/g
          ];
          let foundLink = null;
          for (const pattern of truemoneyPatterns) {
            pattern.lastIndex = 0;
            const matches = messageText.match(pattern);
            if (matches && matches.length > 0) {
              foundLink = matches[0];
              if (!foundLink.startsWith('https://'))
                foundLink = 'https://' + foundLink;
              break;
            }
          }
          if (foundLink) {
            console.log(`🎉 บอท ${botTelegramPhone} (API Key: ${apiKey}) พบลิงก์ซอง: ${foundLink}`);
            await processLink(foundLink, apiKey);
          }
        } catch (error) {
          console.error(`⚠️ ข้อผิดพลาดใน event handler สำหรับ ${botTelegramPhone} (API Key: ${apiKey}):`, error.message);
          if (error.message.includes('Not connected') || error.message.includes('Connection')) {
            console.log(`🔄 บอท ${botTelegramPhone} (API Key: ${apiKey}) กำลังรอการเชื่อมต่อกลับมา...`);
            await delay(5000);
          }
        }
      },
      new NewMessage({ incoming: true, outgoing: true })
    );
    const me = await client.getMe();
    console.log(`✅ บอท ${botTelegramPhone} (${me.firstName || 'N/A'}) (API Key: ${apiKey}) พร้อมทำงาน`);
    activeClients.set(botTelegramPhone, { client, apiKey });
    const intervalId = setInterval(async () => {
      const currentClientData = activeClients.get(botTelegramPhone);
      if (!currentClientData || !currentClientData.client || !currentClientData.client.connected) {
        console.log(`⚠️ บอท ${botTelegramPhone} (API Key ${apiKey}) ไม่ได้เชื่อมต่อหรือถูกลบ - หยุดการตรวจสอบสถานะ.`);
        clearInterval(intervalId);
        activeClients.delete(botTelegramPhone);
        return;
      }
      try {
        await currentClientData.client.getMe();
      } catch (error) {
        console.log(`🔄 บอท ${botTelegramPhone} (API Key ${apiKey}) การตรวจสอบการเชื่อมต่อล้มเหลว: ${error.message}.`);
      }
    }, 60000);
  } catch (error) {
    console.error(`❌ ข้อผิดพลาดในการเริ่มบอท ${botTelegramPhone} (API Key: ${apiKey}):`, error.message);
  }
}

async function getStatusForApiKey(apiKey) {
  const apiKeyData = await loadApiKeyData(apiKey);
  if (!apiKeyData) {
    return { statusCode: 404, body: { success: false, message: "ไม่พบ API key" } };
  }

  const now = Date.now();
  if (apiKeyData.apiKeyExpiresAt < now) {
    console.log(`🗑️ API key ${apiKey} หมดอายุแล้ว กำลังลบ...`);
    const claimingPhone = apiKeyData.claimingPhone;
    await removeBotDataForApiKey(apiKey, "API key หมดอายุ");
    await deleteApiKeyData(apiKey);
    if (claimingPhone) {
      const phoneToApiKeyMap = await loadPhoneToApiKeyMap();
      if (phoneToApiKeyMap[claimingPhone] === apiKey) {
        delete phoneToApiKeyMap[claimingPhone];
        await savePhoneToApiKeyMap(phoneToApiKeyMap);
      }
    }
    return { statusCode: 400, body: { success: false, message: "API key หมดอายุแล้ว" } };
  }

  const remainingTime = calculateRemainingTime(apiKeyData.apiKeyExpiresAt);
  const botStatus = apiKeyData.bot && apiKeyData.bot.active && apiKeyData.bot.sessionExpiresAt > now
    ? {
        active: true,
        telegramPhone: apiKeyData.bot.telegramPhone,
        sessionExpiresAt: new Date(apiKeyData.bot.sessionExpiresAt).toISOString(),
        remainingTime: calculateRemainingTime(apiKeyData.bot.sessionExpiresAt)
      }
    : (apiKeyData.bot ? `ไม่ active หรือหมดอายุ (เบอร์บอท: ${apiKeyData.bot.telegramPhone || 'N/A'})` : "ยังไม่มีการล็อกอินบอท");
  return {
    statusCode: 200,
    body: {
      success: true,
      message: "✅ API key ยังใช้งานได้",
      claimingPhone: apiKeyData.claimingPhone,
      totalAmount: apiKeyData.totalAmount || 0,
      apiKeyExpiresAt: new Date(apiKeyData.apiKeyExpiresAt).toISOString(),
      remainingTime,
      botSessionStatus: botStatus,
    }
  };
}

const app = express();
app.use(express.json());
app.use(cors());

// --- API Endpoints ---
app.post("/submit-phone", async (req, res) => {
  const { phone: claimingPhoneInput, apiKey } = req.body;

  if (!claimingPhoneInput || !apiKey) return res.status(400).json({ success: false, message: "กรุณาระบุทั้งเบอร์โทรศัพท์ (สำหรับรับเงิน) และ API key" });
  const phoneRegex = /^0[6-9][0-9]{8}$/;
  if (!phoneRegex.test(claimingPhoneInput)) return res.status(400).json({ success: false, message: "เบอร์โทรศัพท์ (สำหรับรับเงิน) ไม่ถูกต้อง (รูปแบบ 0[6-9]xxxxxxxx)" });
  if (typeof apiKey !== "string" || apiKey.trim() === "") return res.status(400).json({ success: false, message: "API key ไม่ถูกต้อง" });

  try {
    const phoneToApiKeyMap = await loadPhoneToApiKeyMap();
    let apiKeyExpiresAt;

    // Validate API key with external service
    const checkApiUrl = `https://api.cyber-safe.cloud/api/deletelimit/${encodeURIComponent(apiKey)}/10`;
    console.log(`📞 Calling external service to validate key: ${checkApiUrl}`);
    try {
      const response = await fetch(checkApiUrl);
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        const message = errorData.message || `External key validation failed with status: ${response.status}`;
        if (errorData.status === "error" && errorData.reason === "notkey") {
          console.warn(`⚠️ External service rejected key ${apiKey} (reason: ${errorData.reason})`);
          return res.status(400).json({ success: false, message: `API key ${apiKey} ไม่ถูกต้องตามระบบภายนอก` });
        } else {
          console.warn(`⚠️ External key ${apiKey} validation failed: ${message}`);
          return res.status(400).json({ success: false, message: `ไม่สามารถตรวจสอบความถูกต้องของ key: ${message}` });
        }
      } else {
        const keyData = await response.json();
        if (keyData.status !== 'succeed' || keyData.key !== apiKey) {
          console.warn(`⚠️ External key ${apiKey} is invalid according to the external service. Response:`, keyData);
          return res.status(400).json({ success: false, message: "API key ไม่ถูกต้องตามข้อมูลจากระบบภายนอก" });
        }

        // Parse the expiry time from the response
        const timeString = keyData.time;
        const [datePart, timePart] = timeString.split(' ');
        const [day, month, year] = datePart.split('/');
        const isoFormattedString = `${year}-${month}-${day}T${timePart}`; // Format to ISO
        const expiresAtDate = new Date(isoFormattedString);

        if (isNaN(expiresAtDate.getTime())) {
          console.error(`❌ Could not parse date from external API: "${timeString}"`);
          return res.status(500).json({ success: false, message: "ไม่สามารถประมวลผลวันหมดอายุจากระบบภายนอกได้" });
        }

        apiKeyExpiresAt = expiresAtDate.getTime();
        console.log(`✅ External key ${apiKey} verified. Expires at: ${expiresAtDate.toISOString()}`);
      }
    } catch (error) {
      console.error(`❌ Error calling external service for key ${apiKey}: ${error.message}`);
      return res.status(500).json({ success: false, message: "เกิดข้อผิดพลาดในการติดต่อกับบริการตรวจสอบ Key ภายนอก" });
    }

    if (apiKeyExpiresAt <= Date.now()) {
      return res.status(400).json({ success: false, message: "Key นี้หมดอายุแล้ว" });
    }

    if (phoneToApiKeyMap[claimingPhoneInput]) {
      return res.status(400).json({ success: false, message: `เบอร์รับเงิน ${claimingPhoneInput} นี้ถูกลงทะเบียนกับ API key อื่นแล้ว (${phoneToApiKeyMap[claimingPhoneInput].substring(0,8)}...)` });
    }

    const newApiKeyData = {
      apiKey: apiKey,
      claimingPhone: claimingPhoneInput,
      apiKeyExpiresAt: apiKeyExpiresAt,
      totalAmount: 0,
      bot: null,
    };

    await saveApiKeyData(apiKey, newApiKeyData);

    phoneToApiKeyMap[claimingPhoneInput] = apiKey;
    await savePhoneToApiKeyMap(phoneToApiKeyMap);

    return res.status(200).json({
      success: true,
      message: `✅ เบอร์รับเงิน ${claimingPhoneInput} ถูกผูกกับ API key ${apiKey} เรียบร้อย`,
      apiKey,
      expiresAt: new Date(apiKeyExpiresAt).toISOString(),
    });
  } catch (error) {
    console.error("❌ Error submitting phone:", error.message, error.stack);
    return res.status(500).json({ success: false, message: "เกิดข้อผิดพลาดในการบันทึกข้อมูล" });
  }
});

app.get("/status/:apiKey", async (req, res) => {
  const { apiKey } = req.params;
  try {
    const result = await getStatusForApiKey(apiKey);
    return res.status(result.statusCode).json(result.body);
  } catch (error) {
    console.error(`❌ Error checking API key ${apiKey} status:`, error.message);
    return res.status(500).json({ success: false, message: "เกิดข้อผิดพลาดในการตรวจสอบสถานะ" });
  }
});

app.get("/status-by-phone/:claimingPhone", async (req, res) => {
  const { claimingPhone } = req.params;
  const phoneRegex = /^0[6-9][0-9]{8}$/;
  if (!phoneRegex.test(claimingPhone)) return res.status(400).json({ success: false, message: "เบอร์โทรศัพท์ (สำหรับรับเงิน) ไม่ถูกต้อง (0xxxxxxxxx)" });

  try {
    const phoneToApiKeyMap = await loadPhoneToApiKeyMap();
    const apiKey = phoneToApiKeyMap[claimingPhone];
    if (!apiKey) {
      return res.status(404).json({ success: false, message: `ไม่พบ API key ที่ลงทะเบียนกับเบอร์รับเงิน ${claimingPhone}` });
    }

    const result = await getStatusForApiKey(apiKey);
    return res.status(result.statusCode).json(result.body);
  } catch (error) {
    console.error(`❌ Error checking status by phone ${claimingPhone}:`, error.message);
    return res.status(500).json({ success: false, message: "เกิดข้อผิดพลาดในการตรวจสอบสถานะ" });
  }
});

function validateTelegramPhoneNumber(phone) {
  const phoneRegex = /^\+66[0-9]{9}$/;
  return phoneRegex.test(phone);
}

app.post("/bot-login", async (req, res) => {
  const { phone: botTelegramPhoneInput, code, apiKey } = req.body;

  if (!botTelegramPhoneInput) return res.status(400).json({ success: false, message: "กรุณาระบุเบอร์โทรศัพท์ของบอท (+66xxxxxxxxx)" });
  if (!validateTelegramPhoneNumber(botTelegramPhoneInput)) return res.status(400).json({ success: false, message: "เบอร์โทรศัพท์ของบอทไม่ถูกต้อง (รูปแบบ +66xxxxxxxxx)" });
  if (!apiKey) return res.status(400).json({ success: false, message: "กรุณาระบุ API key ที่ต้องการให้บอทนี้ใช้งาน" });

  let client;
  try {
    let apiKeyData = await loadApiKeyData(apiKey);
    if (!apiKeyData) return res.status(404).json({ success: false, message: `ไม่พบ API Key: ${apiKey} กรุณา /submit-phone เพื่อลงทะเบียน API Key นี้ก่อน` });

    const now = Date.now();
    if (apiKeyData.apiKeyExpiresAt < now) return res.status(400).json({ success: false, message: `API Key ${apiKey} หมดอายุแล้ว` });

    if (apiKeyData.bot && apiKeyData.bot.active && apiKeyData.bot.telegramPhone !== botTelegramPhoneInput && apiKeyData.bot.sessionExpiresAt > now) {
      return res.status(400).json({
        success: false,
        message: `API Key ${apiKey} นี้มีบอทอื่น (${apiKeyData.bot.telegramPhone}) ใช้งานอยู่แล้วและยัง active อยู่`,
      });
    }

    let currentSessionString = "";
    if (apiKeyData.bot && apiKeyData.bot.telegramPhone === botTelegramPhoneInput) {
      currentSessionString = apiKeyData.bot.sessionString || "";
    } else if (apiKeyData.bot) {
      console.log(`API Key ${apiKey} กำลังจะถูกใช้โดยบอทใหม่ ${botTelegramPhoneInput}. บอทเดิม (ถ้ามี): ${apiKeyData.bot.telegramPhone}`);
    }

    client = new TelegramClient(new StringSession(currentSessionString), API_ID, API_HASH,
      { connectionRetries: 5, useWSS: false, floodSleepThreshold: 120, requestTimeout: 15000 }
    );
    console.log(`🔌 พยายามเชื่อมต่อ Telegram สำหรับบอท ${botTelegramPhoneInput} (API Key: ${apiKey})`);
    await client.connect();
    if (!code) { // Step 1: Initiate login, send code
      try {
        const sendCodeResult = await client.invoke(
          new Api.auth.SendCode({ phoneNumber: botTelegramPhoneInput, apiId: API_ID, apiHash: API_HASH, settings: new Api.CodeSettings({}) })
        );
        if (!sendCodeResult || !sendCodeResult.phoneCodeHash) throw new Error("ไม่ได้รับ phoneCodeHash จาก Telegram");
        apiKeyData.bot = {
          ...(apiKeyData.bot || {}),
          telegramPhone: botTelegramPhoneInput,
          sessionString: client.session.save(),
          phoneCodeHash: sendCodeResult.phoneCodeHash,
          createdAt: now,
          sessionExpiresAt: now + (15 * 60 * 1000), // Temp expiry for code entry
          active: false,
        };
        await saveApiKeyData(apiKey, apiKeyData);
        return res.status(200).json({ success: true, message: `✅ ส่งรหัส OTP ไปยัง ${botTelegramPhoneInput} แล้ว กรุณากรอกรหัส.` });
      } catch (error) {
        if (client && client.connected) await client.disconnect();
        console.error(`❌ Error sending code for ${botTelegramPhoneInput} (API Key ${apiKey}):`, error);
        let message = `เกิดข้อผิดพลาดในการส่งรหัสยืนยัน: ${error.message || error.errorMessage}`;
        if (error.errorMessage === "PHONE_NUMBER_INVALID") message = "เบอร์โทรศัพท์บอทไม่ถูกต้องหรือไม่ลงทะเบียนกับ Telegram";
        if (error.errorMessage === "FLOOD_WAIT" || error.errorMessage?.startsWith("FLOOD_WAIT_")) message = `เบอร์นี้ถูกจำกัดชั่วคราว กรุณารอ ${error.seconds || 'สักครู่'} วินาที`;
        return res.status(400).json({ success: false, message });
      }
    } else { // Step 2: Complete login with code
      if (!apiKeyData.bot || !apiKeyData.bot.phoneCodeHash || apiKeyData.bot.telegramPhone !== botTelegramPhoneInput) {
        if (client && client.connected) await client.disconnect();
        return res.status(400).json({ success: false, message: "Session ไม่ถูกต้อง, ไม่พบ phoneCodeHash, หรือเบอร์โทรศัพท์บอทไม่ตรงกัน กรุณาเริ่มใหม่" });
      }
      try {
        await client.invoke(
          new Api.auth.SignIn({ phoneNumber: botTelegramPhoneInput, phoneCodeHash: apiKeyData.bot.phoneCodeHash, phoneCode: code })
        );
        apiKeyData.bot.sessionString = client.session.save();
        apiKeyData.bot.phoneCodeHash = null;
        apiKeyData.bot.active = true;
        apiKeyData.bot.createdAt = now;
        apiKeyData.bot.sessionExpiresAt = apiKeyData.apiKeyExpiresAt;
        await saveApiKeyData(apiKey, apiKeyData);
        await startBotClient(client, botTelegramPhoneInput, apiKey);
        const sessionExpiryDate = new Date(apiKeyData.bot.sessionExpiresAt).toLocaleString('th-TH');
        return res.status(200).json({
          success: true,
          message: `✅ ล็อกอินบอท ${botTelegramPhoneInput} สำเร็จ. Session จะหมดอายุพร้อมกับ API Key ในวันที่ ${sessionExpiryDate}`,
          sessionExpiresAt: new Date(apiKeyData.bot.sessionExpiresAt).toISOString(),
        });
      } catch (error) {
        if (client && client.connected) await client.disconnect();
        console.error(`❌ Error signing in for ${botTelegramPhoneInput} (API Key ${apiKey}):`, error);
        let message = `เกิดข้อผิดพลาดในการยืนยันรหัส: ${error.message || error.errorMessage}`;
        if (error.errorMessage === "PHONE_CODE_INVALID") message = "รหัสยืนยันไม่ถูกต้อง";
        if (error.errorMessage === "PHONE_CODE_EXPIRED") message = "รหัสยืนยันหมดอายุ กรุณาเริ่มใหม่";
        if (error.errorMessage === "SESSION_PASSWORD_NEEDED") message = "บัญชีนี้มีการยืนยันสองชั้น (2FA) กรุณาปิดใช้งานชั่วคราวหรือใช้บัญชีอื่น";
        apiKeyData.bot.active = false;
        apiKeyData.bot.phoneCodeHash = null;
        await saveApiKeyData(apiKey, apiKeyData);
        return res.status(400).json({ success: false, message });
      }
    }
  } catch (error) {
    if (client && client.connected) {
      try {
        await client.disconnect();
      } catch (e) {
        console.error("Error disconnecting client in main catch:", e);
      }
    }
    console.error(`❌ ข้อผิดพลาดร้ายแรงใน /bot-login สำหรับ ${botTelegramPhoneInput} (API Key: ${apiKey}):`, error.message, error.stack);
    return res.status(500).json({ success: false, message: `เกิดข้อผิดพลาดร้ายแรง: ${error.message}` });
  }
});

app.get("/total-bots", async (req, res) => {
  try {
    const apiKeyFilenames = await getAllApiKeyFilenames();
    let activeBotCount = 0;
    const now = Date.now();
    for (const apiKey of apiKeyFilenames) {
      const apiKeyData = await loadApiKeyData(apiKey);
      if (apiKeyData && apiKeyData.bot && apiKeyData.bot.active && apiKeyData.bot.sessionExpiresAt > now) activeBotCount++;
    }
    return res.status(200).json({ success: true, message: "✅ ดึงจำนวนบอทที่ใช้งานอยู่เรียบร้อย", totalActiveBots: activeBotCount });
  } catch (error) {
    console.error("❌ Error fetching total bots:", error.message);
    return res.status(500).json({ success: false, message: "เกิดข้อผิดพลาดในการดึงจำนวนบอท" });
  }
});

app.get("/online-bots", async (req, res) => {
  try {
    const onlineBotCount = Array.from(activeClients.values()).filter(c => c.client && c.client.connected).length;
    return res.status(200).json({
      success: true,
      message: "✅ ดึงจำนวนบอทที่ออนไลน์อยู่เรียบร้อย",
      onlineBotCount: onlineBotCount
    });
  } catch (error) {
    console.error("❌ Error fetching online bots:", error.message);
    return res.status(500).json({
      success: false,
      message: "เกิดข้อผิดพลาดในการดึงจำนวนบอทออนไลน์"
    });
  }
});

app.delete("/remove-bot/:apiKeyToRemove", async (req, res) => {
  const { apiKeyToRemove } = req.params;
  if (!apiKeyToRemove) return res.status(400).json({ success: false, message: "กรุณาระบุ API key ของบอทที่ต้องการลบ session" });
  try {
    const apiKeyData = await loadApiKeyData(apiKeyToRemove);
    if (!apiKeyData) return res.status(404).json({ success: false, message: `ไม่พบ API Key: ${apiKeyToRemove}` });
    if (!apiKeyData.bot) return res.status(404).json({ success: false, message: `ไม่พบข้อมูล session บอทสำหรับ API Key ${apiKeyToRemove}` });

    const botPhone = apiKeyData.bot.telegramPhone;
    await removeBotDataForApiKey(apiKeyToRemove, "ผู้ใช้ร้องขอการลบ");

    return res.status(200).json({ success: true, message: `✅ ล้างข้อมูล bot session สำหรับ API Key ${apiKeyToRemove} (บอท ${botPhone || 'N/A'}) เรียบร้อย` });
  } catch (error) {
    console.error(`❌ Error removing bot session for API Key ${apiKeyToRemove}:`, error.message);
    return res.status(500).json({ success: false, message: "เกิดข้อผิดพลาดในการลบ bot session" });
  }
});

app.delete("/remove-phone/:claimingPhoneToRemove", async (req, res) => {
  const { claimingPhoneToRemove } = req.params;
  const phoneRegex = /^0[6-9][0-9]{8}$/;
  if (!phoneRegex.test(claimingPhoneToRemove)) return res.status(400).json({ success: false, message: "เบอร์โทรศัพท์ (สำหรับรับเงิน) ไม่ถูกต้อง (0xxxxxxxxx)" });
  try {
    const phoneToApiKeyMap = await loadPhoneToApiKeyMap();
    const apiKey = phoneToApiKeyMap[claimingPhoneToRemove];
    if (!apiKey) return res.status(404).json({ success: false, message: `ไม่พบเบอร์รับเงิน ${claimingPhoneToRemove} ที่ลงทะเบียนในระบบ` });

    await removeBotDataForApiKey(apiKey, `เบอร์รับเงิน ${claimingPhoneToRemove} ถูกลบ`);
    await deleteApiKeyData(apiKey);
    delete phoneToApiKeyMap[claimingPhoneToRemove];
    await savePhoneToApiKeyMap(phoneToApiKeyMap);
    console.log(`🗑️ ลบ API Key ${apiKey} และเบอร์รับเงิน ${claimingPhoneToRemove} ออกจากระบบเรียบร้อย`);

    return res.status(200).json({ success: true, message: `✅ ลบเบอร์รับเงิน ${claimingPhoneToRemove} และ API key ${apiKey} ที่เกี่ยวข้องเรียบร้อย` });
  } catch (error) {
    console.error(`❌ Error removing claiming phone ${claimingPhoneToRemove}:`, error.message);
    return res.status(500).json({ success: false, message: "เกิดข้อผิดพลาดในการลบเบอร์" });
  }
});

// Store active TelegramClient instances: Map<botTelegramPhone, {client, apiKey}>
const activeClients = new Map();

async function restoreBotSessions() {
  console.log("🔄 กำลังตรวจสอบและเชื่อมต่อบอทที่เคยล็อกอินไว้...");
  const apiKeyFilenames = await getAllApiKeyFilenames();
  let restoredCount = 0;
  let expiredSessionCount = 0;
  const now = Date.now();
  for (const apiKey of apiKeyFilenames) {
    const apiKeyData = await loadApiKeyData(apiKey);
    if (!apiKeyData || !apiKeyData.bot || !apiKeyData.bot.sessionString || !apiKeyData.bot.telegramPhone || !apiKeyData.bot.active) continue;

    const { telegramPhone: botTelegramPhone, sessionString, sessionExpiresAt } = apiKeyData.bot;
    if (sessionExpiresAt <= now) {
      console.log(`🗑️ Bot session สำหรับ ${botTelegramPhone} (API Key: ${apiKey}) หมดอายุแล้ว`);
      apiKeyData.bot.active = false;
      await saveApiKeyData(apiKey, apiKeyData);
      expiredSessionCount++;
      continue;
    }
    if (activeClients.has(botTelegramPhone)) {
      console.log(`ℹ️ บอท ${botTelegramPhone} มีการเชื่อมต่ออยู่แล้ว ข้าม.`);
      continue;
    }

    try {
      const client = new TelegramClient(new StringSession(sessionString), API_ID, API_HASH,
        { connectionRetries: 3, useWSS: false, floodSleepThreshold: 120, requestTimeout: 10000 }
      );
      console.log(`🔌 กำลังเชื่อมต่อบอท ${botTelegramPhone} (API Key: ${apiKey})...`);
      await client.connect();
      const me = await client.getMe();
      if (me) {
        await startBotClient(client, botTelegramPhone, apiKey);
        restoredCount++;
      } else throw new Error("ไม่สามารถยืนยันข้อมูลผู้ใช้ (getMe failed)");
    } catch (error) {
      console.log(`❌ ไม่สามารถเชื่อมต่อบอท ${botTelegramPhone} (API Key: ${apiKey}): ${error.message}`);
      apiKeyData.bot.active = false;
      await saveApiKeyData(apiKey, apiKeyData);
    }
    await delay(1000);
  }
  console.log(`🎯 สรุปการ restore bot: สำเร็จ ${restoredCount} บอท, session หมดอายุ/ล้มเหลว ${expiredSessionCount} บอท`);
}

setInterval(async () => {
  console.log("🧹 เริ่มการล้างข้อมูลที่หมดอายุ...");
  const now = Date.now();
  let cleanedApiKeys = 0;
  let cleanedBotSessions = 0;
  const apiKeyFilenames = await getAllApiKeyFilenames();
  const phoneToApiKeyMap = await loadPhoneToApiKeyMap();
  let mapUpdated = false;

  for (const apiKey of apiKeyFilenames) {
    const apiKeyData = await loadApiKeyData(apiKey);
    if (!apiKeyData) continue;

    if (apiKeyData.apiKeyExpiresAt < now) {
      console.log(`🗑️ API key ${apiKey} (เบอร์รับเงิน ${apiKeyData.claimingPhone || 'N/A'}) หมดอายุ. กำลังลบ...`);
      await removeBotDataForApiKey(apiKey, "API key หลักหมดอายุ");
      await deleteApiKeyData(apiKey);
      if (apiKeyData.claimingPhone && phoneToApiKeyMap[apiKeyData.claimingPhone] === apiKey) {
        delete phoneToApiKeyMap[apiKeyData.claimingPhone];
        mapUpdated = true;
      }
      cleanedApiKeys++;
      continue;
    }

    if (apiKeyData.bot && apiKeyData.bot.sessionExpiresAt < now) {
      console.log(`🗑️ Bot session สำหรับ API key ${apiKey} (บอท ${apiKeyData.bot.telegramPhone || 'N/A'}) หมดอายุ. กำลังล้าง...`);
      await removeBotDataForApiKey(apiKey, "Bot session หมดอายุ");
      cleanedBotSessions++;
    }
  }
  if (mapUpdated) await savePhoneToApiKeyMap(phoneToApiKeyMap);
  if (cleanedApiKeys > 0 || cleanedBotSessions > 0) console.log(`🧹 การล้างข้อมูลเสร็จสิ้น: ลบ ${cleanedApiKeys} API keys, ล้าง ${cleanedBotSessions} bot sessions.`);
  else console.log("🧹 ไม่พบข้อมูลหมดอายุในรอบนี้");
}, 60 * 60 * 1000);

(async () => {
  try {
    await ensureDirExists(API_KEY_DATA_DIR);
    await loadPendingKeys().then(savePendingKeys);
    await loadPhoneToApiKeyMap().then(savePhoneToApiKeyMap);
    await restoreBotSessions();

    // SSL configuration
    const sslOptions = {
      cert: readFileSync('/etc/letsencrypt/live/menu.panelaimbot.com/fullchain.pem'),
      key: readFileSync('/etc/letsencrypt/live/menu.panelaimbot.com/privkey.pem')
    };

    // Create HTTPS server
    const server = https.createServer(sslOptions, app);
    server.listen(PORT, () => {
      console.log(`🚀 HTTPS server ทำงานที่ port ${PORT}`);
      console.log(`🔗 ทดสอบ: https://menu.panelaimbot.com:${PORT}/submit-phone`);
    });
  } catch (error) {
    console.error("❌ เกิดข้อผิดพลาดร้ายแรงในการ khởi độngระบบ:", error);
    process.exit(1);
  }
})();
