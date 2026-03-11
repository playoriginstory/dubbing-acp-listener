/**
 * diagnose.mjs
 *
 * Connects to ACP, fetches a job by ID, and inspects:
 * - What phase the job is at
 * - How many memos exist
 * - What methods are available on the job object
 * - Whether createRequirement exists
 *
 * Run with:
 *   node diagnose.mjs <JOB_ID>
 *
 * Example:
 *   node diagnose.mjs 1002834120
 *
 * Uses same env vars as index.js — make sure your .env is loaded.
 * If you use dotenv: node -r dotenv/config diagnose.mjs 1002834120
 */

import pkg from "@virtuals-protocol/acp-node";

const { default: AcpClient, AcpContractClientV2 } = pkg;

const JOB_ID = parseInt(process.argv[2]);

if (!JOB_ID || isNaN(JOB_ID)) {
  console.error("Usage: node diagnose.mjs <JOB_ID>");
  console.error("Example: node diagnose.mjs 1002834120");
  process.exit(1);
}

const RESET  = "\x1b[0m";
const GREEN  = "\x1b[32m";
const RED    = "\x1b[31m";
const YELLOW = "\x1b[33m";
const BOLD   = "\x1b[1m";
const CYAN   = "\x1b[36m";

console.log(`\n${BOLD}=== ACP Job Diagnostic ===${RESET}`);
console.log(`${CYAN}Job ID: ${JOB_ID}${RESET}\n`);

async function main() {
  // Build contract client
  let acpContractClient;
  try {
    acpContractClient = await AcpContractClientV2.build(
      process.env.WHITELISTED_WALLET_PRIVATE_KEY,
      parseInt(process.env.SELLER_ENTITY_ID),
      process.env.SELLER_AGENT_WALLET_ADDRESS
    );
    console.log(`${GREEN}✓ AcpContractClient built${RESET}`);
  } catch (err) {
    console.error(`${RED}✗ AcpContractClientV2.build() failed:${RESET}`, err?.message || err);
    process.exit(1);
  }

  // Fetch job
  let job;
  try {
    job = await acpContractClient.getJobById(JOB_ID);
    if (!job) {
      console.error(`${RED}✗ Job ${JOB_ID} not found${RESET}`);
      process.exit(1);
    }
    console.log(`${GREEN}✓ Job fetched${RESET}\n`);
  } catch (err) {
    console.error(`${RED}✗ getJobById failed:${RESET}`, err?.message || err);
    process.exit(1);
  }

  // ── Phase & Memo Diagnosis ──────────────────────────────────────

  const phase = job.phase ?? job.currentPhase ?? "unknown";
  const memoCount = job.memos?.length ?? "unknown";

  console.log(`${BOLD}── Phase & Memo Status ──${RESET}`);
  console.log(`  phase:      ${YELLOW}${phase}${RESET}`);
  console.log(`  memoCount:  ${YELLOW}${memoCount}${RESET}`);

  if (memoCount === 2 || phase === 1) {
    console.log(`\n  ${RED}⚠ DIAGNOSIS: Job stuck at Phase 1 with ${memoCount} memos.${RESET}`);
    console.log(`  ${RED}  This confirms missing createRequirement() — buyer cannot pay.${RESET}`);
  } else if (phase >= 2) {
    console.log(`\n  ${GREEN}✓ Job progressed past Phase 1 — createRequirement likely OK${RESET}`);
    console.log(`  ${YELLOW}  Issue is probably the processing/delivery timeout${RESET}`);
  }

  // ── Available Methods ───────────────────────────────────────────

  console.log(`\n${BOLD}── Available Methods on job object ──${RESET}`);

  const proto = Object.getPrototypeOf(job);
  const methods = Object.getOwnPropertyNames(proto).filter(m => m !== "constructor");

  for (const method of methods.sort()) {
    const exists = typeof job[method] === "function";
    const icon = exists ? `${GREEN}✓${RESET}` : `${RED}✗${RESET}`;
    console.log(`  ${icon} ${method}`);
  }

  // Also check own properties for any extra methods
  const ownMethods = Object.getOwnPropertyNames(job).filter(k => typeof job[k] === "function");
  if (ownMethods.length > 0) {
    console.log(`\n${BOLD}── Own function properties ──${RESET}`);
    for (const m of ownMethods.sort()) {
      console.log(`  ${GREEN}✓${RESET} ${m}`);
    }
  }

  // ── createRequirement Specific Check ───────────────────────────

  console.log(`\n${BOLD}── createRequirement Check ──${RESET}`);

  if (typeof job.createRequirement === "function") {
    console.log(`  ${GREEN}✓ job.createRequirement EXISTS${RESET}`);

    // Try to inspect its signature
    const fnStr = job.createRequirement.toString().slice(0, 200);
    console.log(`  ${CYAN}Signature preview: ${fnStr}...${RESET}`);
  } else {
    console.log(`  ${RED}✗ job.createRequirement does NOT exist${RESET}`);
    console.log(`  ${YELLOW}  You may need a different SDK version or method name${RESET}`);

    // Look for similar methods
    const allKeys = [
      ...Object.getOwnPropertyNames(proto),
      ...Object.getOwnPropertyNames(job)
    ];
    const similar = allKeys.filter(k =>
      k.toLowerCase().includes("require") ||
      k.toLowerCase().includes("create") ||
      k.toLowerCase().includes("memo") ||
      k.toLowerCase().includes("payment")
    );
    if (similar.length > 0) {
      console.log(`\n  ${YELLOW}Similar methods found:${RESET}`);
      for (const m of similar) {
        console.log(`    - ${m}`);
      }
    }
  }

  // ── Raw Job Data ────────────────────────────────────────────────

  console.log(`\n${BOLD}── Raw Job Fields ──${RESET}`);
  const rawFields = Object.keys(job).filter(k => typeof job[k] !== "function");
  for (const field of rawFields) {
    const val = JSON.stringify(job[field])?.slice(0, 100);
    console.log(`  ${field}: ${CYAN}${val}${RESET}`);
  }

  console.log(`\n${BOLD}=== Diagnostic complete ===${RESET}\n`);
}

main().catch(err => {
  console.error(`${RED}Fatal error:${RESET}`, err);
  process.exit(1);
});