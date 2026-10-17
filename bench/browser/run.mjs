// node run.mjs '<json array of runTest configs>'
import puppeteer from "puppeteer-core";
const configs = JSON.parse(process.argv[2]);
const browser = await puppeteer.launch({
	executablePath: process.env.CHROME,
	headless: true,
	args: ["--no-sandbox"],
});
const page = await browser.newPage();
page.on("console", (m) => { if (!m.text().startsWith("{")) console.error("page:", m.text()); });
await page.goto("http://127.0.0.1:8091/");
await page.waitForFunction(() => typeof window.runTest === "function");
for (const config of configs) {
	const result = await page.evaluate((c) => window.runTest(c), config);
	console.log(JSON.stringify(result));
}
await browser.close();
