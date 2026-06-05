// --- Estado global equivalente a GlobalState ---
const chatState = {
  chatIn: false,
  canSendContinue: true,
  lastClickTime: 0,
  currentDialogId: "",
  loadChatState: null,
  saveChatState: null,
};

// --- Historial local de mensajes ---
const messages = [];

// Funciones para guardar y cargar estado, con fallback a localStorage
async function saveChatState() {
  try {
    // Fallback a localStorage
    localStorage.setItem("chatMessages", JSON.stringify(messages));
    localStorage.setItem("chatState", JSON.stringify(chatState));
  } catch (error) {
    console.error("Error guardando estado en localStorage:", error);
  }
}

async function loadChatState() {
  try {
    let result = { messages: [], state: {} };
    if (typeof chatState.loadChatState === "function") {
      result = await chatState.loadChatState();
    }

    if (result.messages && Array.isArray(result.messages)) {
      messages.splice(0, messages.length, ...result.messages);
    }

    if (result.state && typeof result.state === "object") {
      Object.assign(chatState, result.state);
    }

    // Fallback a localStorage
    const savedMessages = localStorage.getItem("chatMessages");
    const savedState = localStorage.getItem("chatState");

    if (savedMessages) {
      try {
        const parsed = JSON.parse(savedMessages);
        if (Array.isArray(parsed)) {
          messages.splice(0, messages.length, ...parsed);
        }
      } catch (e) {
        console.warn("Error al cargar mensajes de fallback:", e);
      }
    }

    if (savedState) {
      try {
        const parsed = JSON.parse(savedState);
        Object.assign(chatState, parsed);
      } catch (e) {
        console.warn("Error al cargar estado de fallback:", e);
      }
    }
  }
 catch (error) {
    console.error("Error cargando estado:", error);
  }
  
}

function appendMessage(role, text) {
  messages.push({ role, text, timestamp: Date.now() });
  saveChatState();
  renderMessages();
}

function renderMessages() {
  const container = document.querySelector("#chatMessages");
  if (!container) return;
  container.innerHTML = "";
  for (const message of messages) {
    const bubble = document.createElement("div");
    bubble.className = `chat-bubble ${message.role}`;
    bubble.textContent = message.text;
    container.appendChild(bubble);
  }
  container.scrollTop = container.scrollHeight;
}

function setStatus(text) {
  const statusEl = document.querySelector("#statusLabel");
  if (statusEl) statusEl.textContent = text;
}

function setTimerText(text) {
  const timerEl = document.querySelector("#timerLabel");
  if (timerEl) timerEl.textContent = text;
}

function updateSendButtonState() {
  const sendButton = document.querySelector("#sendButton");
  if (!sendButton) return;
  sendButton.disabled = chatState.chatIn;
}

function updateContinueButtonState() {
  const continueButton = document.querySelector("#continueButton");
  if (!continueButton) return;
  continueButton.disabled = chatState.chatIn || !chatState.canSendContinue;
}

function createRequestCommonSendMsg(textMsg, msgType, messageType, systemContent, contentId) {
  return {
    textMsg,
    msgType,
    messageType,
    systemContent,
    contentId,
  };
}

// --- PATRÓN STRATEGY PARA STREAMING ---

// Interface base para estrategias de streaming
class StreamingStrategy {
  async execute(messageElement, chunks) {
    throw new Error("Método execute debe ser implementado");
  }
}
 
// Mostrar por palabra
class WordByWordStrategy extends StreamingStrategy {
  async execute(messageElement, chunks) {
    const fullText = chunks.join("");
    const words = fullText.split(" ");
    for (const word of words) {
      messageElement.textContent += (messageElement.textContent ? " " : "") + word;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }
}

// Contexto para ejecutar estrategias
class StreamingContext {
  constructor(strategy = new WordByWordStrategy()) {
    this.strategy = strategy;
  }

  setStrategy(strategy) {
    this.strategy = strategy;
  }

  async stream(messageElement, chunks) {
    await this.strategy.execute(messageElement, chunks);
  }
}

// Instancia global del contexto
const streamingContext = new StreamingContext();

// Función para cambiar estrategia (útil para el usuario)
function setStreamingStrategy(strategyName) {
  const strategies = {
    word: new WordByWordStrategy()
  };

  const strategy = strategies[strategyName] || strategies.word;
  streamingContext.setStrategy(strategy);
  console.log(`Estrategia de streaming: ${strategyName}`);
}
// Funcion para enviar mensaje al backend 
async function sendMessage(request, history) {
  try {
    // cambio a direccion relativa para evitar problemas de CORS en despliegues sin proxy
    const response = await fetch("/api/chat-stream", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ 
        message: request.textMsg,
        history: messages.map(msg => ({ 
          role: msg.role, 
          text: msg.text
        }))
      })
    });

    if (!response.ok) {
      const data = await response.json();
      console.warn("API error:", data);
      throw new Error(data.error || "Respuesta no válida");
    }

    const data = await response.json();
    return data.reply || getChatPromptText(request);
  } catch (error) {
    console.warn("API fallback:", error);
    return getChatPromptText(request);
  }
}

async function sendStreamingMessage(request, onChunk, onComplete, onError) {
  try {
    // cambio a direccion relativa para evitar problemas de CORS en despliegues sin proxy
    const res = await fetch("/api/chat-stream", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ 
        message: request.textMsg,
        history: messages.map(msg => ({ 
          role: msg.role, 
          text: msg.text 
        }))
       })
    });

    if (!res.ok) {
      throw new Error("Error del servidor");
    }

    const container =
      document.querySelector(
        "#chatMessages"
      );

    const bubble =
      document.createElement("div");
    bubble.className =
      "chat-bubble assistant";
    bubble.textContent = "";
    container.appendChild(bubble);

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let chunks = [];

    // Crear bubble temporal para mostrar streaming
    // borrado appendMessage para evitar la duplicacion de burbujas de texto de la IA
    const lastMessage = messages[messages.length - 1];
    container.scrollTop = container.scrollHeight;

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      const text = decoder.decode(value);
      const lines = text.split("\n");

      for (const line of lines) {
        if (line.startsWith("data: ")) {
          try {
            const data = JSON.parse(line.slice(6));
            if (data.chunk) {
              chunks.push(data.chunk);
              if (onChunk) onChunk(data.chunk);
            }
            if (data.done) {
              break;
            }
          } catch (e) {
            // Ignorar líneas que no sean JSON válido
          }
        }
      }
    }

    // Aplicar estrategia de streaming
    await streamingContext.stream(bubble, chunks);

    // Guardar mensaje completo
    messages.push({
      role: "assistant",
      text: bubble.textContent,
      timestamp: Date.now()
});

saveChatState();

    if (onComplete) onComplete();

  } catch (error) {
    console.error("Error streaming:", error);
    if (onError) onError(error);
  }
}

function getChatPromptText(request) {
  return "Respuesta local de fallback: " + request.textMsg;
}

const chatLimitManager = {
  timeoutId: null,
  intervalId: null,
  remainingTime: 0,
  isCounting: false,
  onFinish: null,

  start(durationMs) {
    this.cancel();
    this.remainingTime = durationMs;
    this.isCounting = true;
    this.timeoutId = setTimeout(() => this.handleFinish(), durationMs);
    this.intervalId = setInterval(() => this.tick(), 250);
    this.tick();
  },

  tick() {
    if (!this.isCounting) return;
    this.remainingTime = Math.max(0, this.remainingTime - 250);
    const seconds = Math.ceil(this.remainingTime / 1000);
    setTimerText(`Continue en: ${seconds}s`);
  },

  cancel() {
    if (this.timeoutId) clearTimeout(this.timeoutId);
    if (this.intervalId) clearInterval(this.intervalId);
    this.timeoutId = null;
    this.intervalId = null;
    this.isCounting = false;
    setTimerText("Timer detenido");
  },

  handleFinish() {
    this.cancel();
    setTimerText("Continue listo");
    if (typeof this.onFinish === "function") this.onFinish();
  },

  setOnFinish(callback) {
    this.onFinish = callback;
  },
};

function onTimerFinish() {
  if (!chatState.chatIn) {
    const now = Date.now();
    if (now - chatState.lastClickTime >= 1000) {
      chatState.lastClickTime = now;
      chatState.canSendContinue = false;
      updateContinueButtonState();

      const lastAssistantMessage = 
      messages
          .filter(m => m.role === "assistant")
          .pop();
      if (!lastAssistantMessage) {
        setStatus("No hay primer mensaje para continuar");
        return;
      }

      chatState.chatIn = true;

      const request = {
        textMsg:
        `Continúa este mensaje:\n${chatState.lastAssistantMessage}`
      };
      
      appendMessage(
        "user",
        "Continue"
        );

      sendStreamingMessage(
        request,
        () => setStatus(
          "El asistente está escribiendo..."
        ),
        () => {
          chatState.chatIn = false;

          updateSendButtonState();

          setStatus("Continuación completada.");
          
          saveChatState();
        },
        (error) => {
          setStatus("Error en la respuesta local.");
          console.error(error);
        }
      );
    }
  }
}

function startChat(dialogId) {
  chatState.chatIn = false;
  chatState.canSendContinue = true;
  chatState.currentDialogId = dialogId;
  updateSendButtonState();
  updateContinueButtonState();
  chatLimitManager.start(5000);
  setStatus("Chat iniciado. Envía un mensaje.");
}

// --- Inicialización de la aplicación de chat ---
function userSendMessage(text) {
  if (!text || chatState.chatIn) return;
  chatState.chatIn = true;
  updateSendButtonState();
  updateContinueButtonState();
  appendMessage("user", text);

  const request = createRequestCommonSendMsg(text, "user", "message", "", "");

  sendStreamingMessage(
    request,
    () => setStatus("El asistente está escribiendo..."),
    () => {
      chatState.chatIn = false;
      updateSendButtonState();
      setStatus("Respuesta completada.");
      saveChatState();
    },
    (error) => {
      chatState.chatIn = false;
      updateSendButtonState();
      setStatus("Error en la respuesta local.");
      console.error(error);
    }
  );
}

async function initChatApp() {
  await loadChatState();
  renderMessages();
  updateSendButtonState();
  updateContinueButtonState();
  setStatus("Listo para chatear.");

  const form = document.querySelector("#chatForm");
  const input = document.querySelector("#messageInput");
  const continueButton = document.querySelector("#continueButton");
  const strategySelector = document.querySelector("#strategySelector");

  if (form && input) {
    form.addEventListener("submit", (event) => {
      event.preventDefault();
      const value = input.value.trim();
      if (!value) return;
      userSendMessage(value);
      input.value = "";
    });
  }

  if (continueButton) {
    continueButton.addEventListener("click", () => {
      if (!chatState.canSendContinue || chatState.chatIn) return;
      chatState.canSendContinue = false;
      updateContinueButtonState();
      onTimerFinish();
    });
  }

  // Establecer estrategia por defecto
  setStreamingStrategy("word");

  const clearButton = document.querySelector(".chat-clean");
  if (clearButton) {
    clearButton.addEventListener("click", async () => {
      await clearMemory();
    });
  }

  chatLimitManager.setOnFinish(() => {
    setStatus("Puedes continuar la conversación.");
    chatState.canSendContinue = true;
    updateContinueButtonState();
    saveChatState();
  });

  startChat("local-dialog-1");
}

class MemoryContext {

  constructor(strategy) {

    this.strategy = strategy;

  }

  setStrategy(strategy) {

    this.strategy = strategy;

  }

  async save(data) {

    return await this.strategy.save(data);

  }

  async load() {

    return await this.strategy.load();

  }

  async clear() {

    return await this.strategy.clear();

  }

}

const safestate = {
  chatIn: chatState.chatIn,
  canSendContinue: chatState.canSendContinue,
  lastClickTime: chatState.lastClickTime,
  currentDialogId: chatState.currentDialogId
};

class LocalStorageMemory {

  async save(data) {

    localStorage.setItem(
      "chatMessages",
      JSON.stringify(data.messages)
    );

    localStorage.setItem(
      "safestate",
      JSON.stringify(data.state)
    );

  }

  async load() {

    const messages =
      JSON.parse(
        localStorage.getItem("chatMessages")
      ) || [];

    const state =
      JSON.parse(
        localStorage.getItem("chatState")
      ) || {};

    return {
      messages,
      state
    };

  }

  async clear() {

    localStorage.removeItem(
      "chatMessages"
    );

    localStorage.removeItem(
      "chatState"
    );

    return {
      cleared: true
    };

  }

}

const memoryContext =
  new MemoryContext(
    new LocalStorageMemory()
  );

// borrar memoria (historial) tanto de la UI como del memory contexty localStorage
async function clearMemory() {
  try {
    const result = await memoryContext.clear();
    if (result.cleared) {
      console.log(" Memoria limpiada");
      messages.splice(0, messages.length);
      renderMessages();
      setStatus("Historial borrado.");
      chatState.chatIn = false;
      chatState.canSendContinue = true;
      updateSendButtonState();
      updateContinueButtonState();
    }
  } catch (error) {
    console.error("Error borrando memoria:", error);
  }
}
window.addEventListener("DOMContentLoaded", initChatApp);