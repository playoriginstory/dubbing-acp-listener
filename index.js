import AcpClient, { AcpContractClient } from "@virtuals-protocol/acp-node";

async function main() {
  const acpClient = new AcpClient({
    acpContractClient: await AcpContractClient.build(
      process.env.WHITELISTED_WALLET_PRIVATE_KEY,
      process.env.SELLER_ENTITY_ID,
      process.env.SELLER_AGENT_WALLET_ADDRESS
    ),
    onNewTask: async (job) => {
      console.log("New job received:", JSON.stringify(job));

      const { videoUrl, targetLanguage } = job.serviceRequirement || {};

      if (!videoUrl || !targetLanguage) {
        console.warn("Missing videoUrl or targetLanguage, marking job failed");
        await job.deliver({
          jobId: job.id.toString(),
          status: "failed",
          dubbedFileUrl: "",
        });
        return;
      }

      try {
        const res = await fetch("https://duelsapp.vercel.app/api/dub", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ videoUrl, targetLanguage, source_lang: "auto" }),
        });

        if (!res.ok) throw new Error(`Dub API failed: ${res.status}`);

        const data = await res.json();
        console.log("Dub result:", data);

        if (!data.dubbedFileUrl) throw new Error("No dubbedFileUrl returned");

        const deliverable = {
          jobId: job.id.toString(),
          status: "completed",
          dubbedFileUrl: data.dubbedFileUrl,
        };

        await job.deliver(deliverable);
        console.log("Job delivered successfully:", deliverable);
      } catch (err) {
        console.error("Error processing job:", err.message || err);
        await job.deliver({
          jobId: job.id.toString(),
          status: "failed",
          dubbedFileUrl: "",
        });
      }
    },
  });

  await acpClient.init();
  console.log("ACP seller listener running...");
  
  acpClient.on("taskSubscribed", (task) => {
    console.log("Subscribed to new task:", task.id);
  });
  
  acpClient.on("newTask", (job) => {
    console.log("Received a new job:", job.id, job.serviceRequirement);
  });