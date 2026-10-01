const express = require("express");
const { Pool } = require("pg");
require("dotenv").config();

const app = express();
app.use(express.json());

const CONFIG = {
  SUBSCRIBER_API:
    "https://growth.thewiseparrot.club/api/v1/whatsapp/subscriber/list",

  CONVERSATION_API:
    "https://growth.thewiseparrot.club/api/v1/whatsapp/get/conversation",

  PHONE_NUMBER_ID: "1354199964441267",

  SUBSCRIBER_LIMIT: 100,

  CHAT_LIMIT: 50,

  BOT_GAP_MINUTES: 10,

  DATABASE_TABLE: "book_of_trips_leads",

  SYNC_INTERVAL_MINUTES: 15
};

const API_TOKEN = process.env.WISE_PARROT_API_TOKEN;

if (!API_TOKEN) {
  console.error(
    "ERROR: WISE_PARROT_API_TOKEN is missing in .env"
  );
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,

  ssl: {
    rejectUnauthorized: false
  }
});

let syncRunning = false;


/* =====================================================
   API RATE LIMIT SETTINGS
   ===================================================== */

const CONVERSATION_REQUEST_GAP_MS = 1500;

const RATE_LIMIT_WAIT_MS = 61000;

const MAX_RATE_LIMIT_RETRIES = 2;

let lastConversationRequestTime = 0;


function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}


async function waitBeforeConversationRequest() {

  const elapsed =
    Date.now() - lastConversationRequestTime;

  const remaining =
    CONVERSATION_REQUEST_GAP_MS - elapsed;

  if (remaining > 0) {
    await sleep(remaining);
  }

  lastConversationRequestTime =
    Date.now();
}


/* =====================================================
   HOME
   ===================================================== */

app.get("/", (req, res) => {

  res.send(
    "Wise Parrot Sync Backend is running!"
  );

});


/* =====================================================
   DATABASE TEST
   ===================================================== */

app.get("/test-db", async (req, res) => {

  try {

    const result =
      await pool.query(
        "SELECT NOW() AS current_time"
      );

    res.json({

      success: true,

      message:
        "Neon database connected successfully",

      databaseTime:
        result.rows[0].current_time

    });

  } catch (error) {

    console.error(
      "Database Error:",
      error.message
    );

    res.status(500).json({

      success: false,

      message:
        "Database connection failed",

      error:
        error.message

    });

  }

});


/* =====================================================
   MANUAL FULL SYNC
   ===================================================== */

app.get("/sync", async (req, res) => {

  if (syncRunning) {

    return res.json({

      success: false,

      message:
        "Sync is already running."

    });

  }

  try {

    const result =
      await runFullSync();

    res.json({

      success: true,

      message:
        "Full sync completed.",

      result:
        result

    });

  } catch (error) {

    console.error(
      "Manual Sync Error:",
      error
    );

    res.status(500).json({

      success: false,

      message:
        "Full sync failed.",

      error:
        error.message

    });

  }

});


/* =====================================================
   FULL SYNC
   ===================================================== */

async function runFullSync() {

  if (syncRunning) {

    console.log(
      "Sync already running. Skipping."
    );

    return {
      skipped: true
    };

  }

  syncRunning = true;

  const startTime =
    Date.now();

  let processed = 0;

  let inserted = 0;

  let updated = 0;

  let failed = 0;

  let skipped = 0;

  try {

    console.log("");

    console.log(
      "=========================================="
    );

    console.log(
      "STARTING FULL WISE PARROT SYNC"
    );

    console.log(
      new Date().toLocaleString(
        "en-IN",
        {
          timeZone:
            "Asia/Kolkata"
        }
      )
    );

    console.log(
      "=========================================="
    );


    /* GET ALL SUBSCRIBERS */

    const subscribers =
      await getAllSubscribers();

    console.log(
      "Total subscribers received:",
      subscribers.length
    );


    /* PROCESS TODAY'S SUBSCRIBERS */

    for (
      const subscriber of subscribers
    ) {

      try {

        if (
          !isToday(
            subscriber.last_message_time
          )
        ) {

          skipped++;

          continue;

        }


        if (
          !subscriber.subscriber_id ||
          !subscriber.chat_id
        ) {

          console.log(
            "Skipping subscriber with missing ID/phone."
          );

          skipped++;

          continue;

        }


        console.log("");

        console.log(
          "------------------------------------------"
        );

        console.log(
          "Processing:",
          subscriber.chat_id
        );

        console.log(
          "Subscriber ID:",
          subscriber.subscriber_id
        );


        /* COMPLETE CONVERSATION */

        const conversationData =
          await getConversation(
            subscriber.chat_id
          );


        if (!conversationData) {

          failed++;

          continue;

        }


        console.log(
          "Conversation records:",
          conversationData.message.length
        );


        /* ANALYSE */

        const extracted =
          extractConversationData(
            conversationData
          );


        /* SAVE */

        const saved =
          await saveToDatabase(
            subscriber,
            extracted
          );


        processed++;


        if (
          saved.action ===
          "INSERTED"
        ) {

          inserted++;

        } else {

          updated++;

        }


        console.log(
          "Database:",
          saved.action,
          "| ID:",
          saved.id
        );


      } catch (error) {

        failed++;

        console.error(
          "Subscriber processing failed:",
          subscriber.chat_id,
          "|",
          error.message
        );

      }

    }


    const duration =
      Math.round(
        (
          Date.now() -
          startTime
        ) / 1000
      );


    console.log("");

    console.log(
      "=========================================="
    );

    console.log(
      "FULL SYNC COMPLETED"
    );

    console.log(
      "Processed:",
      processed
    );

    console.log(
      "Inserted:",
      inserted
    );

    console.log(
      "Updated:",
      updated
    );

    console.log(
      "Failed:",
      failed
    );

    console.log(
      "Skipped:",
      skipped
    );

    console.log(
      "Duration:",
      duration,
      "seconds"
    );

    console.log(
      "=========================================="
    );


    return {

      processed:
        processed,

      inserted:
        inserted,

      updated:
        updated,

      failed:
        failed,

      skipped:
        skipped,

      durationSeconds:
        duration

    };


  } finally {

    syncRunning =
      false;

  }

}


/* =====================================================
   GET ALL SUBSCRIBERS
   ===================================================== */

async function getAllSubscribers() {

  let offset = 1;

  let pageNumber = 1;

  let allSubscribers = [];


  while (true) {

    console.log(
      "Fetching subscriber page:",
      pageNumber,
      "| Offset:",
      offset
    );


    const data =
      await getSubscribers(
        offset
      );


    if (!data) {
      break;
    }


    const subscribers =
      data.message || [];


    console.log(
      "Subscribers received:",
      subscribers.length
    );


    if (
      subscribers.length === 0
    ) {

      break;

    }


    allSubscribers =
      allSubscribers.concat(
        subscribers
      );


    const nextOffset =
      data.next_offset ??
      data.nextOffset ??
      null;


    if (
      nextOffset === null ||
      nextOffset === undefined ||
      nextOffset === ""
    ) {

      break;

    }


    const next =
      Number(nextOffset);


    if (
      isNaN(next) ||
      next === offset
    ) {

      break;

    }


    offset =
      next;

    pageNumber++;


    if (
      pageNumber > 1000
    ) {

      console.log(
        "Subscriber pagination safety limit reached."
      );

      break;

    }

  }


  return allSubscribers;

}


/* =====================================================
   SUBSCRIBER API
   ===================================================== */

async function getSubscribers(offset) {

  const response =
    await fetch(
      CONFIG.SUBSCRIBER_API,
      {

        method:
          "POST",

        headers: {

          "Content-Type":
            "application/json"

        },

        body:
          JSON.stringify({

            apiToken:
              API_TOKEN,

            phone_number_id:
              CONFIG.PHONE_NUMBER_ID,

            limit:
              CONFIG.SUBSCRIBER_LIMIT,

            offset:
              offset,

            orderBy:
              1

          })

      }
    );


  const body =
    await response.text();


  console.log(
    "Subscriber API Status:",
    response.status
  );


  if (
    !response.ok
  ) {

    console.error(
      "Subscriber API Error:",
      body
    );

    return null;

  }


  try {

    return JSON.parse(body);

  } catch (error) {

    console.error(
      "Subscriber JSON Error:",
      error.message
    );

    return null;

  }

}


/* =====================================================
   CONVERSATION API
   ===================================================== */

async function getConversation(
  phoneNumber
) {

  let offset = 1;

  let pageNumber = 1;

  let allMessages = [];


  while (true) {

    console.log(
      "Conversation page:",
      pageNumber,
      "| Phone:",
      phoneNumber,
      "| Offset:",
      offset
    );


    let response;

    let body;

    let rateLimitRetries = 0;


    /* ================================================
       RATE LIMIT RETRY
       ================================================ */

    while (true) {

      await waitBeforeConversationRequest();


      response =
        await fetch(
          CONFIG.CONVERSATION_API,
          {

            method:
              "POST",

            headers: {

              "Content-Type":
                "application/json"

            },

            body:
              JSON.stringify({

                apiToken:
                  API_TOKEN,

                phone_number_id:
                  CONFIG.PHONE_NUMBER_ID,

                phone_number:
                  phoneNumber,

                limit:
                  CONFIG.CHAT_LIMIT,

                offset:
                  offset

              })

          }
        );


      body =
        await response.text();


      console.log(
        "Conversation API Status:",
        response.status
      );


      const isRateLimit =
        body
          .toLowerCase()
          .includes(
            "api rate limit/minute exceeded"
          );


      if (isRateLimit) {

        if (
          rateLimitRetries >=
          MAX_RATE_LIMIT_RETRIES
        ) {

          console.error(
            "Rate limit retry limit reached:",
            phoneNumber
          );

          return null;

        }


        rateLimitRetries++;


        console.log(
          `API rate limit reached. Waiting 61 seconds before retry ${rateLimitRetries}/${MAX_RATE_LIMIT_RETRIES}...`
        );


        await sleep(
          RATE_LIMIT_WAIT_MS
        );


        continue;

      }


      if (!response.ok) {

        console.error(
          "Conversation API Error:",
          body
        );

        return null;

      }


      break;

    }


    let data;


    try {

      data =
        JSON.parse(body);

    } catch (error) {

      console.error(
        "Conversation JSON Error:",
        error.message
      );

      return null;

    }


    const pageMessages =
      data.message || [];


    console.log(
      "Records received:",
      pageMessages.length
    );


    allMessages =
      allMessages.concat(
        pageMessages
      );


    const nextOffset =
      data.next_offset ??
      data.nextOffset ??
      null;


    if (
      nextOffset === null ||
      nextOffset === undefined ||
      nextOffset === ""
    ) {

      break;

    }


    const next =
      Number(nextOffset);


    if (
      isNaN(next) ||
      next === offset
    ) {

      break;

    }


    offset =
      next;

    pageNumber++;


    if (
      pageNumber > 1000
    ) {

      console.log(
        "Conversation pagination safety limit reached."
      );

      break;

    }

  }


  console.log(
    "COMPLETE CONVERSATION RECORDS:",
    allMessages.length
  );


  return {

    status:
      "1",

    message:
      allMessages,

    nextOffset:
      null

  };

}


/* =====================================================
   EXTRACT EVERYTHING
   ===================================================== */

function extractConversationData(data) {

  const messages =
    data.message || [];


  let assignedAgent = "";

  let chatMessages = [];

  let labelHistory = [];

  let sequenceHistory = [];

  let notes = [];

  let sequenceResponse = "NO";


  messages.forEach(function(msg) {

    const content =
      String(
        msg.message_content || ""
      );

    const time =
      msg.conversation_time || "";


    if (
      content.indexOf(
        "Label added:"
      ) === 0
    ) {

      labelHistory.push(

        time +
        " - Added: " +
        content
          .replace(
            "Label added:",
            ""
          )
          .trim()

      );

    }


    if (
      content.indexOf(
        "Label removed:"
      ) === 0
    ) {

      labelHistory.push(

        time +
        " - Removed: " +
        content
          .replace(
            "Label removed:",
            ""
          )
          .trim()

      );

    }


    if (
      content.indexOf(
        "Subscribed to sequence:"
      ) === 0
    ) {

      sequenceHistory.push(

        time +
        " - Subscribed: " +
        content
          .replace(
            "Subscribed to sequence:",
            ""
          )
          .trim()

      );

    }


    if (
      content.indexOf(
        "Unsubscribed from sequence:"
      ) === 0
    ) {

      sequenceHistory.push(

        time +
        " - Unsubscribed: " +
        content
          .replace(
            "Unsubscribed from sequence:",
            ""
          )
          .trim()

      );

    }


    if (
      content.indexOf(
        "AI Agent attached a note:"
      ) === 0
    ) {

      notes.push(

        time +
        " - " +
        content
          .replace(
            "AI Agent attached a note:",
            ""
          )
          .trim()

      );

    }


    if (
      content.indexOf(
        "Conversation was assigned to"
      ) === 0
    ) {

      assignedAgent =
        content
          .replace(
            "Conversation was assigned to",
            ""
          )
          .trim();

    }

  });


  messages.forEach(function(msg) {

    const sender =
      msg.sender || "";


    if (
      sender !== "user" &&
      sender !== "bot" &&
      sender !== "ai_agent"
    ) {

      return;

    }


    const text =
      extractMessageText(
        msg.message_content || ""
      );


    if (!text) {

      return;

    }


    let senderName;


    if (
      sender === "user"
    ) {

      senderName =
        "User";

    } else if (
      sender === "bot"
    ) {

      senderName =
        "Bot";

    } else {

      senderName =
        msg.agent_name ||
        "AI Agent";

    }


    chatMessages.push({

      time:
        msg.conversation_time || "",

      sender:
        sender,

      senderName:
        senderName,

      text:
        text

    });

  });


  const last50 =
    chatMessages.slice(-50);


  const chatHistory =
    last50
      .map(function(chat) {

        return (

          chat.time +
          " - " +
          chat.senderName +
          ": " +
          chat.text

        );

      })
      .join("\n");


  let lastSequenceMessageIndex =
    -1;


  for (
    let i = 0;
    i < messages.length;
    i++
  ) {

    if (
      messages[i].sender ===
      "sequence"
    ) {

      lastSequenceMessageIndex =
        i;

    }

  }


  if (
    lastSequenceMessageIndex !== -1
  ) {

    for (
      let i =
        lastSequenceMessageIndex + 1;

      i < messages.length;

      i++
    ) {

      if (
        messages[i].sender ===
        "user"
      ) {

        sequenceResponse =
          "YES";

        break;

      }

    }

  }


  const botResponseAnalysis =
    analyzeBotResponses(
      messages
    );


  return {

    assignedAgent:
      assignedAgent,

    chatHistory:
      chatHistory,

    labelHistory:
      labelHistory.join("\n"),

    sequenceHistory:
      sequenceHistory.join("\n"),

    notes:
      notes.join("\n"),

    sequenceResponse:
      sequenceResponse,

    botResponseAnalysis:
      botResponseAnalysis

  };

}


/* =====================================================
   BOT RESPONSE ANALYSIS
   ===================================================== */

function analyzeBotResponses(
  messages
) {

  const events = [];


  messages.forEach(function(msg) {

    const sender =
      msg.sender || "";


    const time =
      parseConversationTime(
        msg.conversation_time
      );


    if (!time) {

      return;

    }


    const content =
      String(
        msg.message_content || ""
      );


    if (
      sender === "user"
    ) {

      events.push({

        type:
          "user",

        time:
          time,

        rawTime:
          msg.conversation_time,

        content:
          content

      });

      return;

    }


    if (
      sender === "bot" ||
      sender === "ai_agent"
    ) {

      events.push({

        type:
          "bot",

        time:
          time,

        rawTime:
          msg.conversation_time,

        content:
          content,

        agentName:
          msg.agent_name ||
          ""

      });

      return;

    }


    if (
      sender === "system"
    ) {

      const eventType =
        getSystemEventType(
          content
        );


      if (eventType) {

        events.push({

          type:
            eventType,

          time:
            time,

          rawTime:
            msg.conversation_time,

          content:
            content,

          agentName:
            extractAgentName(
              content
            )

        });

      }

    }

  });


  events.sort(function(a, b) {

    return (
      a.time.getTime() -
      b.time.getTime()
    );

  });


  const results = [];


  for (
    let i = 1;
    i < events.length;
    i++
  ) {

    const current =
      events[i];

    const previous =
      events[i - 1];


    if (
      previous.type !== "bot" ||
      current.type !== "bot"
    ) {

      continue;

    }


    const gapMinutes =
      (
        current.time.getTime() -
        previous.time.getTime()
      ) /
      60000;


    if (
      gapMinutes <=
      CONFIG.BOT_GAP_MINUTES
    ) {

      continue;

    }


    let userResponse =
      "NO";

    let currentUserIndex =
      -1;


    for (
      let j = i + 1;
      j < events.length;
      j++
    ) {

      if (
        events[j].type ===
        "user"
      ) {

        userResponse =
          "YES";

        currentUserIndex =
          j;

        break;

      }

    }


    let previousUserIndex =
      -1;


    for (
      let j = i - 1;
      j >= 0;
      j--
    ) {

      if (
        events[j].type ===
        "user"
      ) {

        previousUserIndex =
          j;

        break;

      }

    }


    let botMessagesBetween =
      0;


    if (
      previousUserIndex !== -1 &&
      currentUserIndex !== -1
    ) {

      for (
        let j =
          previousUserIndex + 1;

        j <
          currentUserIndex;

        j++
      ) {

        if (
          events[j].type ===
          "bot"
        ) {

          botMessagesBetween++;

        }

      }

    }


    let lastBeforeUser =
      "N/A";


    if (
      currentUserIndex !== -1
    ) {

      for (
        let j =
          currentUserIndex - 1;

        j >= 0;

        j--
      ) {

        const event =
          events[j];


        if (
          event.type ===
          "sequence"
        ) {

          lastBeforeUser =
            "Sequence";

          break;

        }


        if (
          event.type ===
          "agent"
        ) {

          lastBeforeUser =
            "Agent: " +
            (
              event.agentName ||
              "Unknown"
            );

          break;

        }


        if (
          event.type ===
          "bot"
        ) {

          lastBeforeUser =
            "Bot";

          break;

        }

      }

    }


    let block = "";


    block +=
      "Bot → Bot Gap: " +
      formatMinutes(
        gapMinutes
      ) +
      "\n";


    block +=
      "Response After Bot: " +
      userResponse +
      "\n";


    block +=
      "Bot Messages Between User Replies: " +
      botMessagesBetween +
      "\n";


    block +=
      "Last Message Before User Reply: " +
      lastBeforeUser;


    results.push(
      block
    );

  }


  if (
    results.length === 0
  ) {

    return (
      "No Bot → Bot gap > 10 minutes"
    );

  }


  return results.join(
    "\n\n--------------------\n\n"
  );

}


/* =====================================================
   SYSTEM EVENT TYPE
   ===================================================== */

function getSystemEventType(
  content
) {

  if (
    content.indexOf(
      "Subscribed to sequence:"
    ) === 0
  ) {

    return "sequence";

  }


  if (
    content.indexOf(
      "Conversation was assigned to"
    ) === 0
  ) {

    return "agent";

  }


  return null;

}


/* =====================================================
   AGENT NAME
   ===================================================== */

function extractAgentName(
  content
) {

  if (
    content.indexOf(
      "Conversation was assigned to"
    ) === 0
  ) {

    return content
      .replace(
        "Conversation was assigned to",
        ""
      )
      .trim();

  }


  return "";

}


/* =====================================================
   PARSE TIME
   ===================================================== */

function parseConversationTime(
  value
) {

  if (!value) {

    return null;

  }


  const date =
    new Date(
      String(value)
        .replace(
          " ",
          "T"
        ) +
        "+05:30"
    );


  if (
    isNaN(
      date.getTime()
    )
  ) {

    return null;

  }


  return date;

}


/* =====================================================
   FORMAT MINUTES
   ===================================================== */

function formatMinutes(
  minutes
) {

  const rounded =
    Math.round(
      minutes
    );


  if (
    rounded < 60
  ) {

    return (
      rounded +
      " min"
    );

  }


  const hours =
    Math.floor(
      rounded / 60
    );


  const remaining =
    rounded % 60;


  if (
    remaining === 0
  ) {

    return (
      hours +
      " hr"
    );

  }


  return (
    hours +
    " hr " +
    remaining +
    " min"
  );

}


/* =====================================================
   EXTRACT MESSAGE TEXT
   ===================================================== */

function extractMessageText(
  content
) {

  if (!content) {

    return "";

  }


  if (
    typeof content !==
    "string"
  ) {

    return "";

  }


  let obj;


  try {

    obj =
      JSON.parse(
        content
      );

  } catch (error) {

    return content.trim();

  }


  try {

    const text =
      obj.entry?.[0]
        ?.changes?.[0]
        ?.value?.messages?.[0]
        ?.text?.body;


    if (text) {

      return text.trim();

    }

  } catch (error) {}


  try {

    const title =
      obj.entry?.[0]
        ?.changes?.[0]
        ?.value?.messages?.[0]
        ?.interactive
        ?.button_reply
        ?.title;


    if (title) {

      return title.trim();

    }

  } catch (error) {}


  try {

    const title =
      obj.entry?.[0]
        ?.changes?.[0]
        ?.value?.messages?.[0]
        ?.interactive
        ?.list_reply
        ?.title;


    if (title) {

      return title.trim();

    }

  } catch (error) {}


  try {

    const text =
      obj.text?.body;


    if (text) {

      return text.trim();

    }

  } catch (error) {}


  try {

    const text =
      obj.interactive
        ?.body
        ?.text;


    if (text) {

      return text.trim();

    }

  } catch (error) {}


  return "";

}


/* =====================================================
   SAVE / UPDATE DATABASE
   ===================================================== */

async function saveToDatabase(
  subscriber,
  extracted
) {

  const subscriberId =
    String(
      subscriber.subscriber_id ||
      ""
    ).trim();


  if (!subscriberId) {

    throw new Error(
      "Subscriber ID is missing."
    );

  }


  const fullName =
    (
      subscriber.first_name ||
      ""
    ) +
    " " +
    (
      subscriber.last_name ||
      ""
    );


  const query = `

    INSERT INTO book_of_trips_leads (

      date,

      subscriber_id,

      name,

      phone,

      last_message_time,

      assign_agent,

      chat_history,

      label_history,

      message_sequence_history,

      notes,

      sequence_response,

      bot_response_analysis,

      created_at,

      updated_at

    )

    VALUES (

      CURRENT_DATE,

      $1,

      $2,

      $3,

      $4,

      $5,

      $6,

      $7,

      $8,

      $9,

      $10,

      $11,

      CURRENT_TIMESTAMP,

      CURRENT_TIMESTAMP

    )

    ON CONFLICT (
      subscriber_id
    )

    DO UPDATE SET

      date =
        EXCLUDED.date,

      name =
        EXCLUDED.name,

      phone =
        EXCLUDED.phone,

      last_message_time =
        EXCLUDED.last_message_time,

      assign_agent =
        EXCLUDED.assign_agent,

      chat_history =
        EXCLUDED.chat_history,

      label_history =
        EXCLUDED.label_history,

      message_sequence_history =
        EXCLUDED.message_sequence_history,

      notes =
        EXCLUDED.notes,

      sequence_response =
        EXCLUDED.sequence_response,

      bot_response_analysis =
        EXCLUDED.bot_response_analysis,

      updated_at =
        CURRENT_TIMESTAMP

    RETURNING
      id,
      (
        xmax = 0
      ) AS inserted

  `;


  const values = [

    subscriberId,

    fullName.trim(),

    subscriber.chat_id ||
      "",

    subscriber.last_message_time ||
      null,

    extracted.assignedAgent ||
      "",

    extracted.chatHistory ||
      "",

    extracted.labelHistory ||
      "",

    extracted.sequenceHistory ||
      "",

    extracted.notes ||
      "",

    extracted.sequenceResponse ||
      "NO",

    extracted.botResponseAnalysis ||
      "No Bot → Bot gap > 10 minutes"

  ];


  const result =
    await pool.query(
      query,
      values
    );


  const row =
    result.rows[0];


  return {

    action:
      row.inserted
        ? "INSERTED"
        : "UPDATED",

    id:
      row.id

  };

}


/* =====================================================
   TODAY CHECK
   ===================================================== */

function isToday(
  dateValue
) {

  if (!dateValue) {

    return false;

  }


  const date =
    parseConversationTime(
      dateValue
    );


  if (!date) {

    return false;

  }


  const formatter =
    new Intl.DateTimeFormat(
      "en-CA",
      {

        timeZone:
          "Asia/Kolkata",

        year:
          "numeric",

        month:
          "2-digit",

        day:
          "2-digit"

      }
    );


  const today =
    formatter.format(
      new Date()
    );


  const messageDate =
    formatter.format(
      date
    );


  return (
    today ===
    messageDate
  );

}


/* =====================================================
   AUTOMATIC 15-MINUTE SYNC
   ===================================================== */

setInterval(
  async function() {

    console.log("");

    console.log(
      "15-minute automatic sync triggered."
    );


    if (
      syncRunning
    ) {

      console.log(
        "Previous sync is still running. Skipping."
      );

      return;

    }


    try {

      await runFullSync();

    } catch (error) {

      console.error(
        "Automatic sync error:",
        error.message
      );

    }

  },

  CONFIG.SYNC_INTERVAL_MINUTES *
    60 *
    1000

);


/* =====================================================
   START SERVER
   ===================================================== */

const PORT =
  process.env.PORT ||
  3000;


app.listen(
  PORT,
  () => {

    console.log(
      `Server running on port ${PORT}`
    );

  }
);