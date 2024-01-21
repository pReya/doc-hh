/* To invoke locally:

  1. Run `supabase start` (see: https://supabase.com/docs/reference/cli/supabase-start)
  2. Make an HTTP request:

  curl -i --location --request POST 'http://127.0.0.1:54321/functions/v1/indexer' \
    --header 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0' \
    --header 'Content-Type: application/json' \
    --data '{"name":"Functions"}'

*/

import {
  DOMParser,
  Element,
} from "https://deno.land/x/deno_dom/deno-dom-wasm.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js";
import PromisePool from "https://unpkg.com/native-promise-pool@^3.28.0/edition-deno/index.ts";

console.log("Starting doc-hh Scraper");
console.time("create-index");

interface Metadata {
  title?: string;
  link?: string;
  reference?: string;
  type?: string;
  date?: string;
}

const API_ENDPOINT = "https://www.buergerschaft-hh.de/parldok/formalkriterien";

const extractMetadataFromPage = (document): Metadata[] => {
  const titleEls = document.querySelectorAll("#parldokresult td.title");
  const metadata = [];
  for (const titleEl of titleEls) {
    // Get parent <tr>
    const currentRow = titleEl.parentNode as Element;
    const nextRow = currentRow.nextElementSibling as Element;

    const title = currentRow?.querySelector(".title")?.innerText.trim();
    const link = currentRow?.querySelector(".title a")?.getAttribute("href") ||
      undefined;
    const reference = nextRow?.querySelector("td[headers='result-nummer']")
      ?.innerText;
    const type = nextRow?.querySelector("td[headers='result-typ']")?.innerText;
    const dateParts = nextRow?.querySelector("td[headers='result-datum']")
      ?.innerText?.split(".");
    metadata.push({
      title,
      link,
      reference,
      type,
      date: `${dateParts?.[2] || "1970"}-${dateParts?.[1] || "01"}-${
        dateParts?.[0] || "01"
      }`,
    });
  }
  return metadata;
};

Deno.serve(async (req) => {
  const urlScrapeList = [];

  const initialResponse = await fetch(API_ENDPOINT, {
    method: "GET",
    credentials: "include",
  });
  const initialResponseText = await initialResponse.text();
  const cookies = initialResponse.headers.getSetCookie();

  const initialDocument = new DOMParser().parseFromString(
    initialResponseText,
    "text/html",
  );
  const token = initialDocument?.querySelector('input[name="AFHTOKEN"]')
    ?.getAttribute("value");

  const listResponseOptions = {
    headers: {
      Accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
      "Accept-Language": "en,en-US;q=0.7,de-DE;q=0.3",
      "Content-Type": "application/x-www-form-urlencoded",
      cookie: cookies[0],
    },
    referrer: API_ENDPOINT,
    body:
      "LegislaturperiodenNummer=22&UrheberPersonenId=&UrheberSonstigeId=&DokumententypId=&BeratungsstandId=&Datum=&DatumVon=01.12.2023&DatumBis=01.01.2024&" +
      token,
    method: "POST",
  };

  const scrapeSinglePageForMetadata = async (url: string) => {
    const response = await fetch(url, listResponseOptions);
    const bodyText = await response.text();
    const doc = new DOMParser().parseFromString(
      bodyText,
      "text/html",
    );
    return extractMetadataFromPage(doc);
  };

  const listResponse = await fetch(
    API_ENDPOINT,
    listResponseOptions,
  );

  const bodyText = await listResponse.text();
  const listDocument = new DOMParser().parseFromString(bodyText, "text/html");


  const resultCountGroups = listDocument?.querySelector(".pd_resultcount")
    ?.textContent
    .trim().replace(/\s+/g, " ").match(
      /Dokumente (?<from>\d+) - (?<to>\d+) von (?<overall>\d+)/,
    );

  const resultCount = { ...resultCountGroups?.groups };

  const pages = Math.ceil(
    Number(resultCount?.overall) /
      (Number(resultCount?.to) - Number(resultCount?.from) + 1),
  );

  console.log(`Found ${resultCount.overall} documents on ${pages} pages.`);

  for (let i = 2; i <= pages; i++) {
    urlScrapeList.push(API_ENDPOINT + "/" + i);
  }

  const firstPageData = extractMetadataFromPage(listDocument);

  const pool = new PromisePool(30);

  const allPromises = await Promise.allSettled(
    urlScrapeList.map((url) => pool.open(() => scrapeSinglePageForMetadata(url))),
  );

  const allData: Array<Metadata> = [
    ...firstPageData,
    ...allPromises
      .flatMap(
        (resolvedPromise) =>
          resolvedPromise.status === "fulfilled" ? resolvedPromise.value : null,
      )
      .filter(Boolean) as Metadata[],
  ];


  if (allData.length) {
    try {
      const supabase = createClient(
        Deno.env.get("SUPABASE_URL") ?? "",
        Deno.env.get("SUPABASE_ANON_KEY") ?? "",
        {
          global: {
            headers: { Authorization: req.headers.get("Authorization")! },
          },
        },
      );
      await allData.forEach(async (e) => {
        const dbResult = await supabase
          .from("index")
          .insert(e);
      });
      console.timeEnd("create-index");
    } catch (_) {
    }
  } else {
    console.error("Could not find any metadata");
  }

  return new Response(
    JSON.stringify("done"),
    { headers: { "Content-Type": "application/json" } },
  );
});
