import fs from "fs";
import axios from "axios";
import { HttpsProxyAgent } from "https-proxy-agent";

function parseProxyEntries(rawList = "") {
  return rawList
    .split(/[\n,]/)
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function normalizeProxyEntry(entry) {
  if (!entry) return null;
  const trimmed = entry.trim();
  if (!trimmed) return null;

  if (/^https?:\/\//i.test(trimmed)) {
    return trimmed;
  }

  const parts = trimmed.split(":").map((p) => p.trim()).filter(Boolean);
  if (parts.length >= 4) {
    const [host, port, username, password] = parts;
    if (!host || !port || !username || !password) return null;
    const encodedUser = encodeURIComponent(username);
    const encodedPass = encodeURIComponent(password);
    return `http://${encodedUser}:${encodedPass}@${host}:${port}`;
  }

  if (parts.length === 2) {
    const [host, port] = parts;
    if (!host || !port) return null;
    return `http://${host}:${port}`;
  }

  return null;
}

function loadProxyFileEntries(filePath) {
  if (!filePath) return [];
  try {
    if (!fs.existsSync(filePath)) return [];
    const contents = fs.readFileSync(filePath, "utf8");
    return parseProxyEntries(contents);
  } catch (err) {
    console.warn(`⚠️  Failed to read proxy file ${filePath}: ${err.message}`);
    return [];
  }
}

class ProxyManager {
  constructor() {
    this.proxies = [];
    this.currentIndex = 0;
    this.failedProxies = new Set();
    this.useProxyEnv = process.env.USE_PROXY;
    this.proxyFilePath = process.env.PROXY_FILE || "proxy.txt";
    this.fileProxyEntries = loadProxyFileEntries(this.proxyFilePath);
    this.customProxies = parseProxyEntries(process.env.PROXY_LIST || "");
    if (this.customProxies.length === 0 && this.fileProxyEntries.length > 0) {
      this.customProxies = this.fileProxyEntries;
    }
    if (this.useProxyEnv === undefined && this.customProxies.length > 0) {
      this.useProxy = true;
    } else {
      this.useProxy = this.useProxyEnv === "true";
    }
    this.validateCustomProxies = process.env.PROXY_VALIDATE !== "false";
    this.proxyTestUrl =
      process.env.PROXY_TEST_URL || "https://ipv4.webshare.io/";
    this.maxProxyTests = Number(process.env.PROXY_MAX_TESTS || 15);
  }

  async initialize() {
    if (!this.useProxy) {
      console.log("🔓 Proxy system disabled - using direct connection");
      return;
    }

    if (this.customProxies.length > 0) {
      this.proxies = this.customProxies
        .map(normalizeProxyEntry)
        .filter(Boolean);

      if (this.proxies.length === 0) {
        console.warn("⚠️  Custom proxies provided but none were valid, disabling proxy");
        this.useProxy = false;
        return;
      }

      await this.filterWorkingProxies();
      console.log(`🔐 Loaded ${this.proxies.length} custom proxies`);
      return;
    }

    await this.fetchFreeProxies();
  }

  async fetchFreeProxies() {
    try {
      console.log("🔍 Fetching free proxy list...");
      
      const sources = [
        "https://api.proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
        "https://www.proxy-list.download/api/v1/get?type=http",
      ];

      for (const source of sources) {
        try {
          const response = await axios.get(source, { timeout: 10000 });
          const proxyList = response.data
            .split("\n")
            .map(p => p.trim())
            .filter(p => p && p.includes(":"));
          
          if (proxyList.length > 0) {
            this.proxies.push(...proxyList.slice(0, 20));
            console.log(`✅ Fetched ${proxyList.length} proxies from source`);
            break;
          }
        } catch (err) {
          console.warn(`⚠️  Failed to fetch from source: ${err.message}`);
        }
      }

      if (this.proxies.length === 0) {
        console.warn("⚠️  No proxies found, disabling proxy");
        this.useProxy = false;
      } else {
        console.log(`🔐 Total proxies loaded: ${this.proxies.length}`);
      }
    } catch (err) {
      console.error(`❌ Proxy fetch failed: ${err.message}`);
      this.useProxy = false;
    }
  }

  getNextProxy() {
    if (!this.useProxy || this.proxies.length === 0) {
      return null;
    }

    let attempts = 0;
    while (attempts < this.proxies.length) {
      const proxy = this.proxies[this.currentIndex];
      this.currentIndex = (this.currentIndex + 1) % this.proxies.length;

      if (!this.failedProxies.has(proxy)) {
        return proxy;
      }
      attempts++;
    }

    console.warn("⚠️  All proxies failed, resetting...");
    this.failedProxies.clear();
    return this.proxies[0];
  }

  markProxyAsFailed(proxy) {
    if (proxy) {
      this.failedProxies.add(proxy);
      console.warn(`⚠️  Marking proxy as failed: ${proxy}`);
    }
  }

  getProxyAgent(proxy) {
    if (!proxy) return null;
    
    try {
      const proxyUrl = proxy.startsWith("http") ? proxy : `http://${proxy}`;
      return new HttpsProxyAgent(proxyUrl);
    } catch (err) {
      console.warn(`⚠️  Invalid proxy format: ${proxy}`);
      return null;
    }
  }

  async testProxy(proxy) {
    try {
      const agent = this.getProxyAgent(proxy);
      await axios.get(this.proxyTestUrl, {
        timeout: 5000,
        httpAgent: agent,
        httpsAgent: agent,
      });
      return true;
    } catch {
      return false;
    }
  }

  async filterWorkingProxies() {
    if (!this.validateCustomProxies || this.proxies.length === 0) {
      return;
    }

    console.log(
      `🔍 Testing up to ${this.maxProxyTests || this.proxies.length} proxy(ies) via ${this.proxyTestUrl}`,
    );

    const tested = [];
    let checked = 0;

    for (const proxy of this.proxies) {
      if (this.maxProxyTests && checked >= this.maxProxyTests) {
        tested.push(proxy);
        continue;
      }

      checked++;
      const ok = await this.testProxy(proxy);
      if (ok) {
        tested.push(proxy);
      } else {
        console.warn(`   ⚠️  Proxy failed health check: ${proxy}`);
      }
      await new Promise((r) => setTimeout(r, 200));
    }

    if (tested.length === 0) {
      console.warn(
        "❌ All tested proxies failed. Keeping original list to avoid empty rotation.",
      );
      return;
    }

    if (tested.length !== this.proxies.length) {
      console.log(
        `✅ ${tested.length}/${this.proxies.length} proxies passed health check`,
      );
    }

    this.proxies = tested;
  }

  getStats() {
    return {
      total: this.proxies.length,
      failed: this.failedProxies.size,
      active: this.proxies.length - this.failedProxies.size,
      enabled: this.useProxy,
    };
  }
}

export default ProxyManager;
