const puppeteer = require('puppeteer');

const wait = (ms) => new Promise(r => setTimeout(r, ms));

(async () => {
  const browser = await puppeteer.launch();
  const page = await browser.newPage();
  
  page.on('pageerror', err => {
    console.log('PAGE_ERROR:', err.message);
  });
  
  page.on('console', msg => {
    if (msg.type() === 'error') {
      console.log('CONSOLE_ERROR:', msg.text());
    }
  });

  await page.goto('http://localhost:5173/static/');
  await wait(1000);
  
  // Click Entity browser
  await page.evaluate(() => {
    document.querySelectorAll('.sidebar-link').forEach(el => {
      if (el.innerText.includes('Entity Browser')) el.click();
    });
  });
  await wait(1000);

  // Drag the post_attachments node
  const node = await page.evaluateHandle(() => {
    return Array.from(document.querySelectorAll('div')).find(d => d.innerText.includes('post_attachments'));
  });
  
  if (node) {
    const box = await node.boundingBox();
    if (box) {
      await page.mouse.move(box.x + 10, box.y + 10);
      await page.mouse.down();
      await page.mouse.move(box.x + 50, box.y + 50, { steps: 10 });
      await page.mouse.up();
    }
  }

  await wait(500);
  await browser.close();
})();
