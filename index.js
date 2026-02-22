const AcpClient = require("@virtuals-protocol/acp-node").default;
const { AcpContractClient } = require("@virtuals-protocol/acp-node");

async function main() {
  const acpClient = new AcpClient({
    acpContractClient: await AcpContractClient.build(
      process.env.WHITELISTED_WALLET_PRIVATE_KEY,
      process.env.SELLER_ENTITY_ID,
      process.env.SELLER_AGENT_WALLET_ADDRESS
    ),
    onNewTask: async (job) => {
      console.log("New job received:", JSON.stringify(job));

      const { videoUrl, target_lang } = job.serviceRequirement || {};

      const res = await fetch("https://duelsapp.vercel.app/api/dub", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ videoUrl, target_lang, source_lang: "auto" }),
      });

      const data = await res.json();
      console.log("Dub result:", data);

      await acpClient.deliverJob(job.id, `Dubbing started: ${data.dubbing_id}`);
    },
  });

  await acpClient.init();
  console.log("ACP seller listener running...");
}

main().catch(console.error);