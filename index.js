const { ACPClient } = require("@virtuals-protocol/acp-node");

const client = new ACPClient({
  walletPrivateKey: process.env.WHITELISTED_WALLET_PRIVATE_KEY,
  agentWalletAddress: process.env.AGENT_WALLET_ADDRESS,
});

client.onJob("dub_video", async (job) => {
  const { videoUrl, target_lang } = job.requirement;

  const res = await fetch("https://duelsapp.vercel.app/api/dub", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ videoUrl, target_lang, source_lang: "auto" }),
  });

  const data = await res.json();

  await job.complete({
    result: `Dubbing started with ID: ${data.dubbing_id}`,
  });
});

client.start();
console.log("ACP listener running...");