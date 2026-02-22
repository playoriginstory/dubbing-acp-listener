import AcpClient, { AcpContractClient } from "@virtuals-protocol/acp-node";
console.log("All env keys:", Object.keys(process.env).filter(k => k.includes('WALLET')));
console.log("Private Key:", process.env.WHITELISTED_WALLET_PRIVATE_KEY?.slice(0, 6) + "..."); 
// Map user-friendly language names to codes
const LANGUAGES = [
  { code: "en", name: "English" }, { code: "hi", name: "Hindi" },
  { code: "pt", name: "Portuguese" }, { code: "zh", name: "Chinese" },
  { code: "es", name: "Spanish" }, { code: "fr", name: "French" },
  { code: "de", name: "German" }, { code: "ja", name: "Japanese" },
  { code: "ar", name: "Arabic" }, { code: "ru", name: "Russian" },
  { code: "ko", name: "Korean" }, { code: "id", name: "Indonesian" },
  { code: "it", name: "Italian" }, { code: "nl", name: "Dutch" },
  { code: "tr", name: "Turkish" }, { code: "pl", name: "Polish" },
  { code: "sv", name: "Swedish" }, { code: "fil", name: "Filipino" },
  { code: "ms", name: "Malay" }, { code: "ro", name: "Romanian" },
  { code: "uk", name: "Ukrainian" }, { code: "el", name: "Greek" },
  { code: "cs", name: "Czech" }, { code: "da", name: "Danish" },
  { code: "fi", name: "Finnish" }, { code: "bg", name: "Bulgarian" },
  { code: "hr", name: "Croatian" }, { code: "sk", name: "Slovak" },
  { code: "ta", name: "Tamil" },
];

function getLanguageCode(input) {
  if (!input) return null;
  const entry = LANGUAGES.find(
    (l) => l.code.toLowerCase() === input.toLowerCase() || l.name.toLowerCase() === input.toLowerCase()
  );
  return entry ? entry.code : null;
}

async function main() {
  const acpClient = new AcpClient({
    acpContractClient: await AcpContractClient.build(
      process.env.WHITELISTED_WALLET_PRIVATE_KEY,
      process.env.SELLER_ENTITY_ID,
      process.env.SELLER_AGENT_WALLET_ADDRESS
    ),
    onNewTask: async (job) => {
      console.log(`Received job #${job.id}`);
      const { videoUrl, targetLanguage } = job.serviceRequirement || {};

      if (!videoUrl || !targetLanguage) {
        console.warn(`Job #${job.id} missing videoUrl or targetLanguage`);
        await job.deliver({ jobId: job.id.toString(), status: "failed", dubbedFileUrl: "" });
        return;
      }

      const langCode = getLanguageCode(targetLanguage);
      if (!langCode) {
        console.warn(`Job #${job.id} has unsupported target language: ${targetLanguage}`);
        await job.deliver({ jobId: job.id.toString(), status: "failed", dubbedFileUrl: "" });
        return;
      }

      console.log(`Job #${job.id} -> Dubbing to ${langCode}`);

      try {
        const res = await fetch("https://duelsapp.vercel.app/api/dub", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ videoUrl, targetLanguage: langCode, source_lang: "auto" }),
        });

        if (!res.ok) throw new Error(`Dub API failed with status ${res.status}`);
        const data = await res.json();
        console.log(`Dub API response for job #${job.id}:`, data);

        const deliverable = {
          jobId: job.id.toString(),
          status: data.dubbedFileUrl ? "completed" : "failed",
          dubbedFileUrl: data.dubbedFileUrl || "",
        };

        await job.deliver(deliverable);
        console.log(`Job #${job.id} delivered:`, deliverable);
      } catch (err) {
        console.error(`Error processing job #${job.id}:`, err.message || err);
        await job.deliver({ jobId: job.id.toString(), status: "failed", dubbedFileUrl: "" });
      }
    },
  });

  await acpClient.init();
  console.log("ACP seller listener running and ready to receive jobs...");
}

main().catch((err) => console.error("Listener failed to start:", err));