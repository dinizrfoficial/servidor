const http = require("http");
const WebSocket = require("ws");

// ============================================================
// FIREBASE ADMIN
// ============================================================

const admin = require("firebase-admin");
const fs = require("fs");

const FIREBASE_DATABASE_URL =
    process.env.FIREBASE_DATABASE_URL ||
    "https://z-link-talk-default-rtdb.firebaseio.com";

const FIREBASE_SERVICE_ACCOUNT_PATH =
    "/etc/secrets/firebase-service-account.json";

let db = null;
let firebaseReady = false;

function initializeFirebase() {
    try {
        if (!fs.existsSync(FIREBASE_SERVICE_ACCOUNT_PATH)) {
            throw new Error(
                `Arquivo de credencial não encontrado: ${FIREBASE_SERVICE_ACCOUNT_PATH}`
            );
        }

        const serviceAccount =
            JSON.parse(
                fs.readFileSync(
                    FIREBASE_SERVICE_ACCOUNT_PATH,
                    "utf8"
                )
            );

        admin.initializeApp({
            credential: admin.credential.cert(
                serviceAccount
            ),
            databaseURL:
                FIREBASE_DATABASE_URL
        });

        db = admin.database();

        firebaseReady = true;

        console.log(
            "[FIREBASE] Admin SDK inicializado"
        );

        console.log(
            `[FIREBASE] Database URL: ${FIREBASE_DATABASE_URL}`
        );

        return true;

    } catch (error) {

        console.error(
            "[FIREBASE] Falha ao inicializar:",
            error.message
        );

        return false;
    }
}

async function testFirebaseConnection() {

    if (!firebaseReady || !db) {
        console.error(
            "[FIREBASE] Banco não disponível para teste"
        );
        return false;
    }

    try {

        await db
            .ref("_system/server")
            .update({
                status: "online",
                updatedAt: Date.now(),
                service: "z-link-talk"
            });

        console.log(
            "[FIREBASE] Conexão com Realtime Database OK"
        );

        return true;

    } catch (error) {

        console.error(
            "[FIREBASE] Erro ao gravar no banco:",
            error.message
        );

        firebaseReady = false;

        return false;
    }
}

initializeFirebase();


// ============================================================
// HTTP
// ============================================================

const PORT =
    Number(
        process.env.PORT || 3000
    );

const httpServer =
    http.createServer(
        (req, res) => {

            res.writeHead(
                200,
                {
                    "Content-Type":
                        "application/json; charset=utf-8"
                }
            );

            res.end(
                JSON.stringify({
                    service: "Z-Link Talk",
                    status: "online",
                    firebase: firebaseReady,
                    timestamp: Date.now()
                })
            );
        }
    );


// ============================================================
// WEBSOCKET
// ============================================================

const wss =
    new WebSocket.WebSocketServer({
        server: httpServer,
        maxPayload: 64 * 1024
    });


// ============================================================
// CLIENTES
// ============================================================

// userId -> WebSocket
const clients = new Map();


// ============================================================
// TRANSMISSOR ATUAL
// ============================================================

let activeTransmitterId = null;
let activeTransmitStartedAt = 0;


// ============================================================
// ESTATÍSTICAS DE ÁUDIO
// ============================================================

let audioPacketCount = 0;
let audioBytesRelayed = 0;


// ============================================================
// PROTOCOLO DE ÁUDIO
// ============================================================
//
// [0]   = 0x5A ('Z')
// [1]   = 0x4C ('L')
// [2]   = versão 1
// [3]   = flags
// [4]   = sequência high
// [5]   = sequência low
// [6..] = payload Opus
//

const AUDIO_MAGIC_0 = 0x5A;
const AUDIO_MAGIC_1 = 0x4C;
const AUDIO_VERSION = 1;
const AUDIO_HEADER_SIZE = 6;


// ============================================================
// UTILITÁRIOS
// ============================================================

function isOpen(ws) {

    return (
        ws &&
        ws.readyState === WebSocket.OPEN
    );
}


function clientList() {

    return [
        ...clients.values()
    ]
        .filter(isOpen)
        .map(
            ws => ({
                id: ws.userId,
                name: ws.name
            })
        );
}


function sendJson(
    ws,
    data
) {

    if (!isOpen(ws)) {
        return;
    }

    try {

        ws.send(
            JSON.stringify(data)
        );

    } catch (error) {

        console.error(
            `[JSON TX ERROR] ${error.message}`
        );
    }
}


function broadcastJson(
    data,
    exceptId = null
) {

    const payload =
        JSON.stringify(data);

    for (
        const ws
        of clients.values()
    ) {

        if (
            isOpen(ws) &&
            ws.userId !== exceptId
        ) {

            try {

                ws.send(
                    payload
                );

            } catch (error) {

                console.error(
                    `[BROADCAST ERROR] ${error.message}`
                );
            }
        }
    }
}


// ============================================================
// VALIDAÇÃO DE ÁUDIO
// ============================================================

function isAudioPacket(
    buf
) {

    return (
        Buffer.isBuffer(buf) &&
        buf.length > AUDIO_HEADER_SIZE &&
        buf[0] === AUDIO_MAGIC_0 &&
        buf[1] === AUDIO_MAGIC_1 &&
        buf[2] === AUDIO_VERSION
    );
}


// ============================================================
// CONTROLE DO TRANSMISSOR
// ============================================================

function resetTransmitterIf(
    userId
) {

    if (
        activeTransmitterId !== userId
    ) {
        return;
    }

    console.log(
        `[TX RESET] ${userId}`
    );

    activeTransmitterId = null;
    activeTransmitStartedAt = 0;

    broadcastJson({
        type: "stop_tx",
        from: userId
    });
}


// ============================================================
// JSON / CONTROLE
// ============================================================

function handleJson(
    ws,
    data
) {

    if (
        !data ||
        typeof data !== "object"
    ) {
        return;
    }


    // ========================================================
    // IDENTIFY
    // ========================================================

    if (
        data.type === "identify"
    ) {

        const userId =
            String(
                data.userId || ""
            ).trim();

        if (!userId) {

            console.warn(
                "[IDENTIFY] usuário sem userId"
            );

            return;
        }


        const old =
            clients.get(
                userId
            );


        if (
            old &&
            old !== ws
        ) {

            console.log(
                `[IDENTIFY] reconexão de ${userId}`
            );

            resetTransmitterIf(
                userId
            );

            try {

                old.close(
                    4001,
                    "Reconnected"
                );

            } catch (_) {
            }
        }


        ws.userId =
            userId;

        ws.name =
            String(
                data.name ||
                "Anônimo"
            ).slice(
                0,
                32
            );


        clients.set(
            userId,
            ws
        );


        console.log(
            `[IDENTIFY] ${userId} -> ${ws.name} | usuários: ${clients.size}`
        );


        sendJson(
            ws,
            {
                type: "init",
                id: userId,
                clients:
                    clientList(),

                activeTransmitter:
                    activeTransmitterId
                        ? {
                            id:
                                activeTransmitterId,

                            name:
                                clients.get(
                                    activeTransmitterId
                                )?.name ||
                                activeTransmitterId
                        }
                        : null
            }
        );


        broadcastJson({
            type: "user_list",
            clients:
                clientList()
        });


        return;
    }


    // ========================================================
    // IDENTIFICAÇÃO OBRIGATÓRIA
    // ========================================================

    if (!ws.userId) {
        return;
    }


    // ========================================================
    // UPDATE NAME
    // ========================================================

    if (
        data.type === "update_name"
    ) {

        ws.name =
            String(
                data.name ||
                "Anônimo"
            ).slice(
                0,
                32
            );


        console.log(
            `[NAME] ${ws.userId} -> ${ws.name}`
        );


        broadcastJson({
            type: "user_update",
            id:
                ws.userId,
            name:
                ws.name
        });


        return;
    }


    // ========================================================
    // START TX
    // ========================================================

    if (
        data.type === "start_tx"
    ) {

        if (
            activeTransmitterId &&
            activeTransmitterId !==
                ws.userId
        ) {

            const activeName =
                clients.get(
                    activeTransmitterId
                )?.name ||
                activeTransmitterId;


            console.log(
                `[TX DENIED] ${ws.userId} ` +
                `tentou transmitir; canal ocupado por ` +
                `${activeTransmitterId}`
            );


            sendJson(
                ws,
                {
                    type:
                        "tx_denied",

                    name:
                        activeName
                }
            );


            return;
        }


        activeTransmitterId =
            ws.userId;


        activeTransmitStartedAt =
            Date.now();


        audioPacketCount =
            0;


        audioBytesRelayed =
            0;


        console.log(
            `[TX START] ${ws.userId} (${ws.name})`
        );


        broadcastJson(
            {
                type:
                    "start_tx",

                from:
                    ws.userId,

                name:
                    ws.name
            }
        );


        return;
    }


    // ========================================================
    // STOP TX
    // ========================================================

    if (
        data.type === "stop_tx"
    ) {

        const duration =
            activeTransmitStartedAt > 0
                ? Date.now() -
                    activeTransmitStartedAt
                : 0;


        console.log(
            `[TX STOP] ${ws.userId} (${ws.name}) | ` +
            `pacotes: ${audioPacketCount} | ` +
            `bytes: ${audioBytesRelayed} | ` +
            `duração: ${duration} ms`
        );


        resetTransmitterIf(
            ws.userId
        );


        return;
    }


    // ========================================================
    // PING APP
    // ========================================================

    if (
        data.type === "ping_app"
    ) {

        sendJson(
            ws,
            {
                type:
                    "pong_app",

                ts:
                    Date.now()
            }
        );

        return;
    }
}


// ============================================================
// NOVA CONEXÃO WEBSOCKET
// ============================================================

wss.on(
    "connection",
    ws => {

        console.log(
            "[WS] Cliente conectado"
        );


        ws.userId =
            null;


        ws.name =
            "Anônimo";


        ws.isAlive =
            true;


        // ====================================================
        // PONG
        // ====================================================

        ws.on(
            "pong",
            () => {
                ws.isAlive = true;
            }
        );


        // ====================================================
        // MESSAGE
        // ====================================================

        ws.on(
            "message",
            (
                data,
                isBinary
            ) => {


                // ============================================
                // ÁUDIO
                // ============================================

                if (
                    isAudioPacket(
                        data
                    )
                ) {

                    if (
                        !ws.userId ||
                        activeTransmitterId !==
                            ws.userId
                    ) {

                        console.warn(
                            `[AUDIO DROP] pacote rejeitado ` +
                            `user=${ws.userId || "não identificado"}`
                        );

                        return;
                    }


                    audioPacketCount++;
                    audioBytesRelayed +=
                        data.length;


                    let delivered =
                        0;


                    // ========================================
                    // RETRANSMISSÃO
                    // ========================================

                    for (
                        const client
                        of clients.values()
                    ) {

                        if (
                            isOpen(client) &&
                            client.userId !==
                                ws.userId
                        ) {

                            try {

                                client.send(
                                    data,
                                    {
                                        binary:
                                            true
                                    }
                                );

                                delivered++;

                            } catch (error) {

                                console.error(
                                    `[AUDIO TX ERROR] ` +
                                    `para=${client.userId} ` +
                                    `${error.message}`
                                );
                            }
                        }
                    }


                    // ========================================
                    // LOG
                    // ========================================

                    if (
                        audioPacketCount === 1 ||
                        audioPacketCount % 100 === 0
                    ) {

                        const opusSize =
                            data.length -
                            AUDIO_HEADER_SIZE;


                        console.log(
                            `[AUDIO] RX #${audioPacketCount} ` +
                            `de=${ws.userId} ` +
                            `bytes=${data.length} ` +
                            `opus=${opusSize} ` +
                            `destinatarios=${delivered}`
                        );
                    }


                    return;
                }


                // ============================================
                // BINARY INVÁLIDO
                // ============================================

                if (isBinary) {

                    console.warn(
                        `[BINARY DROP] pacote binário inválido ` +
                        `bytes=${data.length}`
                    );

                    return;
                }


                // ============================================
                // JSON
                // ============================================

                let message;


                try {

                    message =
                        JSON.parse(
                            data.toString()
                        );

                } catch (error) {

                    console.warn(
                        "[JSON DROP] mensagem inválida"
                    );

                    return;
                }


                handleJson(
                    ws,
                    message
                );
            }
        );


        // ====================================================
        // CLOSE
        // ====================================================

        ws.on(
            "close",
            (
                code,
                reason
            ) => {

                console.log(
                    `[WS] Conexão encerrada: ` +
                    `${ws.userId || "não identificado"} ` +
                    `code=${code} ` +
                    `reason=${reason?.toString() || ""}`
                );


                if (!ws.userId) {
                    return;
                }


                if (
                    clients.get(
                        ws.userId
                    ) === ws
                ) {

                    clients.delete(
                        ws.userId
                    );
                }


                resetTransmitterIf(
                    ws.userId
                );


                broadcastJson({
                    type:
                        "user_list",

                    clients:
                        clientList()
                });
            }
        );


        // ====================================================
        // ERROR
        // ====================================================

        ws.on(
            "error",
            error => {

                console.error(
                    `[WS ERROR] ` +
                    `${ws.userId || "não identificado"}: ` +
                    `${error.message}`
                );
            }
        );
    }
);


// ============================================================
// HEARTBEAT
// ============================================================

setInterval(
    () => {

        for (
            const [
                userId,
                ws
            ]
            of clients
        ) {

            if (
                ws.isAlive === false
            ) {

                console.log(
                    `[HEARTBEAT] removendo conexão morta: ${userId}`
                );


                try {
                    ws.terminate();
                } catch (_) {
                }


                clients.delete(
                    userId
                );


                resetTransmitterIf(
                    userId
                );


                continue;
            }


            ws.isAlive =
                false;


            try {

                ws.ping();

            } catch (_) {
            }
        }

    },
    30_000
);


// ============================================================
// HTTP SERVER START
// ============================================================

httpServer.listen(
    PORT,
    "0.0.0.0",
    async () => {

        console.log(
            `Z-Link Talk Audio Server listening on ${PORT}`
        );


        // ----------------------------------------------------
        // Teste do Firebase
        // ----------------------------------------------------

        await testFirebaseConnection();

        console.log(
            `[STARTUP] Firebase=${firebaseReady ? "OK" : "OFFLINE"}`
        );
    }
);


// ============================================================
// ESTATÍSTICAS DE ÁUDIO
// ============================================================

setInterval(
    () => {

        if (
            audioPacketCount > 0
        ) {

            console.log(
                `[AUDIO STATS] ` +
                `pacotes=${audioPacketCount} ` +
                `bytes=${audioBytesRelayed}`
            );


            audioPacketCount =
                0;


            audioBytesRelayed =
                0;
        }

    },
    60_000
);