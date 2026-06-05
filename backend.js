// Dependencias que se necesita instalar
const express = require("express");
const path = require("path");
const cors = require("cors");
const { GoogleGenerativeAI } = require("@google/generative-ai");

require("dotenv").config();

const app = express();

app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3002;

const GEMINI_KEY = process.env.GEMINI_API_KEY;

if (!GEMINI_KEY) {
  
  console.warn("Falta GEMINI_API_KEY");
  
}

const genAI = new GoogleGenerativeAI(GEMINI_KEY);

const model = genAI.getGenerativeModel({
  model: "gemini-3.1-flash-lite"
});

// archivos estáticos
app.use(express.static(path.join(__dirname)));

app.use("/Fantasia", express.static(path.join(__dirname, "Fantasia"))
);

// abrir index.html
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

// API CHAT CON STREAMING
app.post("/api/chat-stream", async (req, res) => {
  try {
    const { 
      message,
      history = [] } = req.body;

    if (!message || typeof message !== "string") {
      return res.status(400).json({
        error: "El campo message es obligatorio"
      });
    }

    // Convertir historial al formato Gemini
    const contents = history.map(msg => ({

      role:
        msg.role === "assistant"
          ? "model"
          : "user",

      parts: [
        {
          text: msg.text
        }
      ]

    }));

    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");

    const limitedHistory = history.slice(-20); // limita la memoria a los ultimos 20 mensajes

    const conversationContext = limitedHistory
      .map(msg => `${msg.role}: ${msg.text}`)
      .join("\n");
    const prompt = `
      Eres un asistente.
      Historial:
      ${conversationContext}
      Usuario: ${message}
      `;

    const result = await model.generateContentStream(prompt);

    for await (const chunk of result.stream) {
      const text = chunk.text();
      if (text) {
        res.write(`data: ${JSON.stringify({ chunk: text })}\n\n`);
      }
    }

    res.write(`data: ${JSON.stringify({ done: true })}\n\n`);
    res.end();

  } catch (error) {
    console.error("ERROR GEMINI STREAM:", error);
    res.write(`data: ${JSON.stringify({ error: error.message })}\n\n`);
    res.end();
  }
});

// API CHAT (sin streaming)
app.post("/api/chat", async (req, res) => {
  try {
    const { message, history } = req.body;

    if (!message || typeof message !== "string") {
      return res.status(400).json({
        error: "El campo message es obligatorio"
      });
    }

    const conversationContext = history
      .map(msg => {
        return `${msg.role}: ${msg.text}`;
      })
      .join("\n");
    const prompt = `
      Eres un asistente.
      Historial:
      ${conversationContext}
      Usuario: ${message}
      `;

  
    const result = await model.generateContent(prompt);

    const reply = result.response.text();

    res.json({ reply });

  } 
  
  catch (error) {
    console.error("ERROR GEMINI:", error);

    res.status(500).json({
      error: "No se pudo contactar con Gemini",
      details: error.message
    });
  }
});


// iniciar servidor
app.listen(PORT, () => {
  console.log(`Servidor iniciado en http://localhost:${PORT}`);
});