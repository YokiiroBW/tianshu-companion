const bridge = window.AstrBotPluginPage;
const state = document.getElementById("state");
const endpoint = document.getElementById("endpoint");
const reveal = document.getElementById("reveal");
const hide = document.getElementById("hide");
const key = document.getElementById("key");

try {
  await bridge.ready();
  reveal.disabled = false;
  state.textContent = "已连接 AstrBot 管理页面。密钥只在点击后读取。";
} catch {
  state.textContent = "无法访问 AstrBot 管理页面，请重新登录后打开此页。";
}

reveal.addEventListener("click", async () => {
  reveal.disabled = true;
  key.textContent = "";
  try {
    const info = await bridge.apiGet("connection-info");
    if (info.protocol !== "tianshu.bot-adapter/v1" || typeof info.access_key !== "string") {
      throw new Error("invalid response");
    }
    state.textContent = info.listening ? "适配器正在监听。" : "适配器未监听，请检查插件状态和端口。";
    endpoint.hidden = false;
    const address = info.listen_mode === "loopback"
      ? `http://127.0.0.1:${info.port}（仅与平台同机可用）`
      : `http://<插件宿主私网地址>:${info.port}`;
    endpoint.textContent = `模式：${info.listen_mode}；监听：${info.listen_host}:${info.port}。天枢网页地址请填 ${address}。`;
    key.textContent = info.access_key;
    hide.hidden = false;
  } catch {
    state.textContent = "读取失败：请确认当前账号是 AstrBot 管理员并重新登录。";
  } finally {
    reveal.disabled = false;
  }
});

hide.addEventListener("click", () => {
  key.textContent = "";
  hide.hidden = true;
});

window.addEventListener("pagehide", () => { key.textContent = ""; });
