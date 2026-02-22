import pkg from "@virtuals-protocol/acp-node";
const { default: AcpClient, AcpContractClient } = pkg;

console.log("All env keys:", Object.keys(process.env).filter(k => k.includes('WALLET')));
console.log("Private Key:", process.env.WHITELISTED_WALLET_PRIVATE_KEY?.slice(0, 6) + "...");

const LANGUAGES = [
  { code: "en", name: "English" }, { code: "es", name: "Spanish" },
  { code: "fr", name: "French" }, { code: "de", name: "German" },
  { code: "ja", name: "Japanese" }, { code: "zh", name: "Chinese" },
  { code: "pt", name: "Portuguese" }, { code: "hi", name: "Hindi" },
  { code: "ar", name: "Arabic" }, { code: "ru", name: "Russian" },
  { code: "ko", name: "Korean" }, { code: "it", name: "Italian" },
  { code: "nl", name: "Dutch" }, { code: "tr", name: "Turkish" },
  { code: "pl", name: "Polish" }, { code: "sv", name: "Swedish" },
  { code: "fil", name: "Filipino" }, { code: "ms", name: "Malay" },
  { code: "ro", name: "Romanian" }, { code: "uk", name: "Ukrainian" },
  { code: "el", name: "Greek" }, { code: "cs", name: "Czech" },
  { code: "da", name: "Danish" }, { code: "fi", name: "Finnish" },
  { code: "bg", name: "Bulgarian" }, { code: "hr", name: "Croatian" },
  { code: "sk", name: "Slovak" }, { code: "ta", name: "Tamil" },
  { code: "id", name: "Indonesian" },
];

function getLanguageCode(input) {
  if (!input) return null;
  const entry = LANGUAGES.find(
    l => l.code.toLowerCase() === input.toLowerCase() || 
         l.name.toLowerCase() === input.toLowerCase()
  );
  return entry ? entry.code : null;
}

async function main() {
  const acpContractClient = await AcpContractClient.build(
    process.env.WHITELISTED_WALLET_PRIVATE_KEY,
    process.env.SELLER_ENTITY_ID,
    process.env.SELLER_AGENT_WALLET_ADDRESS
  );

  const acpClient = new AcpClient({
    acpContractClient,
    onNewTask: async (job) => {
      console.log("New job received:", JSON.stringify(job));

      const { videoUrl, targetLanguage } = job.serviceRequirement || {};
      const langCode = getLanguageCode(targetLanguage);

      if (!videoUrl || !langCode) {
        await job.deliver({ jobId: job.id.toString(), status: "failed", dubbedFileUrl: "" });
        return;
      }

      try {
        const res = await fetch("https://duelsapp.vercel.app/api/dub", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ videoUrl, target_lang: langCode, source_lang: "auto" }),
        });

        const data = await res.json();
        const deliverable = {
          jobId: job.id.toString(),
          status: "completed",
          dubbedFileUrl: data.dubbedFileUrl || "",
        };

        await job.deliver(deliverable);
        console.log("Job delivered:", deliverable);
      } catch (err) {
        console.error("Error processing job:", err);
        await job.deliver({ jobId: job.id.toString(), status: "failed", dubbedFileUrl: "" });
      }
    },
  });

  await acpClient.init();
  console.log("ACP seller listener running...");
}

main().catch(console.error);