// node profile.mjs '<runTest config json>' -> prints top self-time functions
import puppeteer from "puppeteer-core";
const config = JSON.parse(process.argv[2]);
const browser = await puppeteer.launch({
	executablePath: process.env.CHROME,
	headless: true, args: ["--no-sandbox"],
});
const page = await browser.newPage();
await page.goto("http://127.0.0.1:8091/");
await page.waitForFunction(() => typeof window.runTest === "function");
// warm up (loads and compiles the wasm)
await page.evaluate((c) => window.runTest({ ...c, total: 200 }), config);
const cdp = await page.createCDPSession();
await cdp.send("Profiler.enable");
await cdp.send("Profiler.setSamplingInterval", { interval: 100 });
await cdp.send("Profiler.start");
const result = await page.evaluate((c) => window.runTest(c), config);
const { profile } = await cdp.send("Profiler.stop");
console.log(JSON.stringify(result));
const self = new Map();
const byId = new Map(profile.nodes.map((n) => [n.id, n]));
let total = 0;
for (const n of profile.nodes) {
	const name = `${n.callFrame.functionName || "(anon)"} ${n.callFrame.url.split("/").pop()}`;
	self.set(name, (self.get(name) || 0) + (n.hitCount || 0));
	total += n.hitCount || 0;
}
[...self.entries()].sort((a, b) => b[1] - a[1]).slice(0, 400)
	.forEach(([k, v]) => console.log(`${(100 * v / total).toFixed(1).padStart(5)}% ${k.slice(0, 150)}`));
await browser.close();
