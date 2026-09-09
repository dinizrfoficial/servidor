const http = require("http");
const WebSocket = require("ws");
const fs = require("fs");
const crypto = require("crypto");

// ============================================================
// FIREBASE ADMIN SDK
// ============================================================

const {
    initializeApp,
    cert
} = require("firebase-admin/app");

const {
    getDatabase
} = require("firebase-admin/database");

const {
    getAuth
} = require("firebase-admin/auth");

const FIREBASE_DATABASE_URL =
    process.env.FIREBASE_DATABASE_URL ||
    "https://z-link-talk-default-rtdb.firebaseio.com";

const FIREBASE_SERVICE_ACCOUNT_PATH =
    "/etc/secrets/firebase-service-account.json";

let db = null;
let auth = null;
let firebaseReady = false;

// ============================================================
// FIREBASE
// ============================================================

function initializeFirebase() {
    try {
        if (
            !fs.existsSync(
                FIREBASE_SERVICE_ACCOUNT_PATH
            )
        ) {
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

        initializeApp({
            credential: cert(
                serviceAccount
            ),
            databaseURL:
                FIREBASE_DATABASE_URL
        });

        db =
            getDatabase();

        auth =
            getAuth();

        firebaseReady =
            true;

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

        firebaseReady =
            false;

        db = null;
        auth = null;

        return false;
    }
}

async function testFirebaseConnection() {

    if (
        !firebaseReady ||
        !db
    ) {
        console.error(
            "[FIREBASE] Banco não disponível para teste"
        );

        return false;
    }

    try {

        await db
            .ref("_system/server")
            .update({
                status:
                    "online",

                updatedAt:
                    Date.now(),

                service:
                    "z-link-talk"
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

        return false;
    }
}

initializeFirebase();

// ============================================================
// CONFIGURAÇÃO
// ============================================================

const PORT =
    Number(
        process.env.PORT || 3000
    );

// ============================================================
// WEBSOCKET CLIENTES
// ============================================================
//
// uid -> WebSocket
//

const clients =
    new Map();

// ============================================================
// TRANSMISSOR
// ============================================================

let activeTransmitterId =
    null;

let activeTransmitStartedAt =
    0;

// ============================================================
// ÁUDIO
// ============================================================

let audioPacketCount =
    0;

let audioBytesRelayed =
    0;

// ============================================================
// PROTOCOLO DE ÁUDIO
// ============================================================

const AUDIO_MAGIC_0 =
    0x5A;

const AUDIO_MAGIC_1 =
    0x4C;

const AUDIO_VERSION =
    1;

const AUDIO_HEADER_SIZE =
    6;

// ============================================================
// SESSÕES
// ============================================================
//
// sessions/{uid}
//    sessionId
//    deviceId
//    createdAt
//    updatedAt
//

// ============================================================
// HTTP HELPERS
// ============================================================

function setCors(res) {

    res.setHeader(
        "Access-Control-Allow-Origin",
        "*"
    );

    res.setHeader(
        "Access-Control-Allow-Headers",
        "Content-Type, Authorization"
    );

    res.setHeader(
        "Access-Control-Allow-Methods",
        "GET, POST, OPTIONS"
    );
}

function sendHttpJson(
    res,
    status,
    data
) {

    res.writeHead(
        status,
        {
            "Content-Type":
                "application/json; charset=utf-8",

            "Cache-Control":
                "no-store"
        }
    );

    res.end(
        JSON.stringify(
            data
        )
    );
}

function readJsonBody(
    req
) {

    return new Promise(
        (
            resolve,
            reject
        ) => {

            let body =
                "";

            let finished =
                false;

            req.on(
                "data",
                chunk => {

                    if (
                        finished
                    ) {
                        return;
                    }

                    body +=
                        chunk.toString();

                    if (
                        body.length >
                        16 * 1024
                    ) {

                        finished =
                            true;

                        reject(
                            new Error(
                                "Payload muito grande"
                            )
                        );

                        req.destroy();
                    }
                }
            );

            req.on(
                "end",
                () => {

                    if (
                        finished
                    ) {
                        return;
                    }

                    try {

                        resolve(
                            JSON.parse(
                                body ||
                                "{}"
                            )
                        );

                    } catch (_) {

                        reject(
                            new Error(
                                "JSON inválido"
                            )
                        );
                    }
                }
            );

            req.on(
                "error",
                error => {

                    if (
                        !finished
                    ) {

                        finished =
                            true;

                        reject(
                            error
                        );
                    }
                }
            );
        }
    );
}

// ============================================================
// TOKEN
// ============================================================

function getBearerToken(
    req
) {

    const header =
        req.headers.authorization;

    if (
        !header ||
        !header.startsWith(
            "Bearer "
        )
    ) {

        return null;
    }

    return header
        .substring(7)
        .trim();
}

async function verifyBearerToken(
    req
) {

    const token =
        getBearerToken(
            req
        );

    if (
        !token ||
        !auth
    ) {

        throw new Error(
            "UNAUTHORIZED"
        );
    }

    return auth.verifyIdToken(
        token
    );
}

// ============================================================
// SESSION OPEN
// ============================================================

async function handleSessionOpen(
    req,
    res
) {

    if (
        !firebaseReady ||
        !db ||
        !auth
    ) {

        sendHttpJson(
            res,
            503,
            {
                success:
                    false,

                error:
                    "Serviço temporariamente indisponível"
            }
        );

        return;
    }

    let decoded;

    try {

        decoded =
            await verifyBearerToken(
                req
            );

    } catch (_) {

        sendHttpJson(
            res,
            401,
            {
                success:
                    false,

                error:
                    "Sessão Firebase inválida"
            }
        );

        return;
    }

    let body;

    try {

        body =
            await readJsonBody(
                req
            );

    } catch (error) {

        sendHttpJson(
            res,
            400,
            {
                success:
                    false,

                error:
                    error.message
            }
        );

        return;
    }

    const deviceId =
        String(
            body.deviceId ||
                ""
        ).trim();

    const force =
        body.force === true;

    if (
        deviceId.length <
        8
    ) {

        sendHttpJson(
            res,
            400,
            {
                success:
                    false,

                error:
                    "Identificador do dispositivo inválido"
            }
        );

        return;
    }

    const uid =
        decoded.uid;

    const sessionRef =
        db.ref(
            `sessions/${uid}`
        );

    let currentSession =
        null;

    try {

        const snapshot =
            await sessionRef.get();

        if (
            snapshot.exists()
        ) {

            currentSession =
                snapshot.val() ||
                {};
        }

    } catch (error) {

        console.error(
            "[SESSION] erro lendo sessão:",
            error.message
        );

        sendHttpJson(
            res,
            500,
            {
                success:
                    false,

                error:
                    "Não foi possível consultar a sessão"
            }
        );

        return;
    }

    // ========================================================
    // MESMO DISPOSITIVO
    // ========================================================

    if (
        currentSession &&
        currentSession.deviceId ===
            deviceId &&
        currentSession.sessionId
    ) {

        await sessionRef.update({
            updatedAt:
                Date.now()
        });

        console.log(
            `[SESSION RESUME] uid=${uid} device=${deviceId}`
        );

        let username =
            decoded.name ||
            "";

        try {

            const profile =
                await db
                    .ref(
                        `users/${uid}`
                    )
                    .get();

            if (
                profile.exists()
            ) {

                username =
                    profile.val()?.username ||
                    username;
            }

        } catch (_) {
            // Usa o nome do token se a leitura falhar.
        }

        sendHttpJson(
            res,
            200,
            {
                success:
                    true,

                sessionId:
                    currentSession.sessionId,

                username,

                resumed:
                    true
            }
        );

        return;
    }

    // ========================================================
    // OUTRO DISPOSITIVO SEM FORCE
    // ========================================================
    //
    // Impede que um aparelho antigo que ficou offline
    // volte sozinho e expulse o aparelho atual.
    //

    if (
        currentSession &&
        currentSession.deviceId &&
        currentSession.deviceId !==
            deviceId &&
        !force
    ) {

        console.log(
            `[SESSION DENIED] uid=${uid} ` +
            `deviceAtual=${currentSession.deviceId} ` +
            `deviceSolicitado=${deviceId}`
        );

        sendHttpJson(
            res,
            409,
            {
                success:
                    false,

                code:
                    "SESSION_TAKEN",

                error:
                    "Sua conta está conectada em outro dispositivo"
            }
        );

        return;
    }

    // ========================================================
    // NOVA SESSÃO
    // ========================================================

    const sessionId =
        crypto
            .randomBytes(
                24
            )
            .toString(
                "hex"
            );

    const newSession = {
        sessionId,
        deviceId,

        createdAt:
            Date.now(),

        updatedAt:
            Date.now()
    };

    /*
     * Guarda a sessão nova.
     */
    await sessionRef.set(
        newSession
    );

    /*
     * Se já havia WebSocket da sessão anterior,
     * avisa e derruba o dispositivo antigo.
     */
    const oldWs =
        clients.get(
            uid
        );

    if (
        oldWs &&
        oldWs.sessionId !==
            sessionId
    ) {

        console.log(
            `[SESSION REVOKE] uid=${uid}`
        );

        try {

            sendJson(
                oldWs,
                {
                    type:
                        "session_revoked",

                    reason:
                        "Você entrou com outro dispositivo."
                }
            );

            oldWs.close(
                4001,
                "Session replaced"
            );

        } catch (_) {
        }
    }

    let username =
        decoded.name ||
        "";

    try {

        const profile =
            await db
                .ref(
                    `users/${uid}`
                )
                .get();

        if (
            profile.exists()
        ) {

            username =
                profile.val()?.username ||
                username;
        }

    } catch (_) {
    }

    console.log(
        `[SESSION OPEN] uid=${uid} ` +
        `device=${deviceId} ` +
        `force=${force}`
    );

    sendHttpJson(
        res,
        200,
        {
            success:
                true,

            sessionId,

            username,

            resumed:
                false
        }
    );
}

// ============================================================
// SESSION VALIDATION FOR WEBSOCKET
// ============================================================

async function validateStoredSession(
    uid,
    sessionId,
    deviceId
) {

    if (
        !db ||
        !uid ||
        !sessionId ||
        !deviceId
    ) {

        return false;
    }

    try {

        const snapshot =
            await db
                .ref(
                    `sessions/${uid}`
                )
                .get();

        if (
            !snapshot.exists()
        ) {

            return false;
        }

        const session =
            snapshot.val() ||
            {};

        return (
            session.sessionId ===
                sessionId &&
            session.deviceId ===
                deviceId
        );

    } catch (error) {

        console.error(
            "[SESSION VALIDATE]",
            error.message
        );

        return false;
    }
}

// ============================================================
// USERNAME
// ============================================================

function normalizeUsername(
    username
) {

    return String(
        username || ""
    )
        .trim()
        .toLowerCase()
        .replace(
            /\s+/g,
            " "
        );
}

function validateUsername(
    username
) {

    if (
        username.length <
        3
    ) {

        return (
            "O nome de usuário deve ter pelo menos 3 caracteres"
        );
    }

    if (
        username.length >
        20
    ) {

        return (
            "O nome de usuário deve ter no máximo 20 caracteres"
        );
    }

    if (
        !/^[A-Za-zÀ-ÿ0-9 _-]+$/.test(
            username
        )
    ) {

        return (
            "O nome de usuário contém caracteres inválidos"
        );
    }

    return null;
}

function usernameKey(
    username
) {

    return normalizeUsername(
        username
    );
}

// ============================================================
// USERNAME RESERVATION
// ============================================================

const USERNAME_RESERVATION_MS =
    2 * 60 * 1000;

// ============================================================
// REGISTRATION RATE LIMIT
// ============================================================

const registrationRateLimit =
    new Map();

const RATE_LIMIT_WINDOW =
    60 * 1000;

const RATE_LIMIT_MAX =
    30;

function getRequestIp(req) {

    const forwarded =
        req.headers[
            "x-forwarded-for"
        ];

    if (
        forwarded
    ) {

        return String(
            forwarded
        )
            .split(",")[0]
            .trim();
    }

    return (
        req.socket?.remoteAddress ||
        "unknown"
    );
}

function isRateLimited(
    req
) {

    const ip =
        getRequestIp(
            req
        );

    const now =
        Date.now();

    let entry =
        registrationRateLimit.get(
            ip
        );

    if (
        !entry
    ) {

        entry = {
            start:
                now,

            count:
                0
        };

        registrationRateLimit.set(
            ip,
            entry
        );
    }

    if (
        now -
            entry.start >
        RATE_LIMIT_WINDOW
    ) {

        entry.start =
            now;

        entry.count =
            0;
    }

    entry.count++;

    return (
        entry.count >
        RATE_LIMIT_MAX
    );
}

// ============================================================
// CHECK REGISTRATION
// ============================================================

async function handleCheckRegistration(
    req,
    res
) {

    if (
        isRateLimited(req)
    ) {

        sendHttpJson(
            res,
            429,
            {
                success:
                    false,

                error:
                    "Muitas tentativas. Aguarde um momento."
            }
        );

        return;
    }

    if (
        !firebaseReady ||
        !db ||
        !auth
    ) {

        sendHttpJson(
            res,
            503,
            {
                success:
                    false,

                error:
                    "Serviço temporariamente indisponível"
            }
        );

        return;
    }

    let body;

    try {

        body =
            await readJsonBody(
                req
            );

    } catch (error) {

        sendHttpJson(
            res,
            400,
            {
                success:
                    false,

                error:
                    error.message
            }
        );

        return;
    }

    const username =
        String(
            body.username ||
                ""
        ).trim();

    const email =
        String(
            body.email ||
                ""
        )
            .trim()
            .toLowerCase();

    const usernameError =
        validateUsername(
            username
        );

    if (
        usernameError
    ) {

        sendHttpJson(
            res,
            400,
            {
                success:
                    false,

                usernameAvailable:
                    false,

                emailAvailable:
                    false,

                usernameExists:
                    false,

                emailExists:
                    false,

                error:
                    usernameError
            }
        );

        return;
    }

    if (
        !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(
            email
        )
    ) {

        sendHttpJson(
            res,
            400,
            {
                success:
                    false,

                usernameAvailable:
                    false,

                emailAvailable:
                    false,

                usernameExists:
                    false,

                emailExists:
                    false,

                error:
                    "E-mail inválido"
            }
        );

        return;
    }

    const key =
        usernameKey(
            username
        );

    let usernameExists =
        false;

    let emailExists =
        false;

    try {

        const snapshot =
            await db
                .ref(
                    `usernames/${key}`
                )
                .get();

        usernameExists =
            snapshot.exists();

    } catch (error) {

        console.error(
            "[REGISTER CHECK] username:",
            error.message
        );

        sendHttpJson(
            res,
            500,
            {
                success:
                    false,

                error:
                    "Não foi possível verificar o nome de usuário"
            }
        );

        return;
    }

    if (
        !usernameExists
    ) {

        try {

            const reservation =
                await db
                    .ref(
                        `usernameReservations/${key}`
                    )
                    .get();

            if (
                reservation.exists()
            ) {

                const value =
                    reservation.val() ||
                    {};

                if (
                    Number(
                        value.expiresAt ||
                            0
                    ) >
                    Date.now()
                ) {

                    usernameExists =
                        true;

                } else {

                    await db
                        .ref(
                            `usernameReservations/${key}`
                        )
                        .remove();
                }
            }

        } catch (error) {

            console.error(
                "[REGISTER CHECK] reservation:",
                error.message
            );
        }
    }

    try {

        await auth.getUserByEmail(
            email
        );

        emailExists =
            true;

    } catch (error) {

        if (
            error?.code ===
            "auth/user-not-found"
        ) {

            emailExists =
                false;

        } else {

            console.error(
                "[REGISTER CHECK] email:",
                error.message
            );

            sendHttpJson(
                res,
                500,
                {
                    success:
                        false,

                    error:
                        "Não foi possível verificar o e-mail"
                }
            );

            return;
        }
    }

    console.log(
        `[REGISTER CHECK] username=${key} ` +
        `usernameExists=${usernameExists} ` +
        `emailExists=${emailExists}`
    );

    sendHttpJson(
        res,
        200,
        {
            success:
                true,

            usernameAvailable:
                !usernameExists,

            emailAvailable:
                !emailExists,

            usernameExists,
            emailExists
        }
    );
}

// ============================================================
// RESERVE USERNAME
// ============================================================

async function handleReserveUsername(
    req,
    res
) {

    if (
        isRateLimited(req)
    ) {

        sendHttpJson(
            res,
            429,
            {
                success:
                    false,

                error:
                    "Muitas tentativas. Aguarde um momento."
            }
        );

        return;
    }

    if (
        !firebaseReady ||
        !db
    ) {

        sendHttpJson(
            res,
            503,
            {
                success:
                    false,

                error:
                    "Serviço temporariamente indisponível"
            }
        );

        return;
    }

    let body;

    try {

        body =
            await readJsonBody(
                req
            );

    } catch (error) {

        sendHttpJson(
            res,
            400,
            {
                success:
                    false,

                error:
                    error.message
            }
        );

        return;
    }

    const username =
        String(
            body.username ||
                ""
        ).trim();

    const key =
        usernameKey(
            username
        );

    const validation =
        validateUsername(
            username
        );

    if (
        validation
    ) {

        sendHttpJson(
            res,
            400,
            {
                success:
                    false,

                error:
                    validation
            }
        );

        return;
    }

    try {

        const existing =
            await db
                .ref(
                    `usernames/${key}`
                )
                .get();

        if (
            existing.exists()
        ) {

            sendHttpJson(
                res,
                409,
                {
                    success:
                        false,

                    error:
                        "Esse nome de usuário já está cadastrado"
                }
            );

            return;
        }

    } catch (error) {

        console.error(
            "[USERNAME RESERVE]",
            error.message
        );

        sendHttpJson(
            res,
            500,
            {
                success:
                    false,

                error:
                    "Não foi possível verificar o nome de usuário"
            }
        );

        return;
    }

    const reservationId =
        crypto
            .randomBytes(
                18
            )
            .toString(
                "hex"
            );

    const reservationRef =
        db.ref(
            `usernameReservations/${key}`
        );

    try {

        const result =
            await reservationRef.transaction(
                current => {

                    const now =
                        Date.now();

                    if (
                        current ==
                        null
                    ) {

                        return {
                            reservationId,

                            expiresAt:
                                now +
                                USERNAME_RESERVATION_MS
                        };
                    }

                    const expiresAt =
                        Number(
                            current.expiresAt ||
                                0
                        );

                    if (
                        expiresAt <=
                        now
                    ) {

                        return {
                            reservationId,

                            expiresAt:
                                now +
                                USERNAME_RESERVATION_MS
                        };
                    }

                    return;
                }
            );

        const saved =
            result.snapshot.val() ||
            {};

        if (
            !result.committed ||
            saved.reservationId !==
                reservationId
        ) {

            sendHttpJson(
                res,
                409,
                {
                    success:
                        false,

                    error:
                        "Esse nome de usuário já está sendo utilizado"
                }
            );

            return;
        }

        console.log(
            `[USERNAME RESERVE] ${key} ` +
            `reservation=${reservationId}`
        );

        sendHttpJson(
            res,
            200,
            {
                success:
                    true,

                usernameKey:
                    key,

                reservationId,

                expiresAt:
                    saved.expiresAt
            }
        );

    } catch (error) {

        console.error(
            "[USERNAME RESERVE]",
            error.message
        );

        sendHttpJson(
            res,
            500,
            {
                success:
                    false,

                error:
                    "Não foi possível reservar o nome"
            }
        );
    }
}

// ============================================================
// FINALIZE REGISTRATION
// ============================================================

async function handleFinalizeRegistration(
    req,
    res
) {

    if (
        !firebaseReady ||
        !db ||
        !auth
    ) {

        sendHttpJson(
            res,
            503,
            {
                success:
                    false,

                error:
                    "Serviço temporariamente indisponível"
            }
        );

        return;
    }

    let decodedToken;

    try {

        decodedToken =
            await verifyBearerToken(
                req
            );

    } catch (_) {

        sendHttpJson(
            res,
            401,
            {
                success:
                    false,

                error:
                    "Sessão inválida ou expirada"
            }
        );

        return;
    }

    let body;

    try {

        body =
            await readJsonBody(
                req
            );

    } catch (error) {

        sendHttpJson(
            res,
            400,
            {
                success:
                    false,

                error:
                    error.message
            }
        );

        return;
    }

    const username =
        String(
            body.username ||
                ""
        ).trim();

    const key =
        usernameKey(
            username
        );

    const reservationId =
        String(
            body.reservationId ||
                ""
        ).trim();

    const validation =
        validateUsername(
            username
        );

    if (
        validation
    ) {

        sendHttpJson(
            res,
            400,
            {
                success:
                    false,

                error:
                    validation
            }
        );

        return;
    }

    if (
        !reservationId
    ) {

        sendHttpJson(
            res,
            400,
            {
                success:
                    false,

                error:
                    "Reserva de nome inválida"
            }
        );

        return;
    }

    let firebaseUser;

    try {

        firebaseUser =
            await auth.getUser(
                decodedToken.uid
            );

    } catch (_) {

        sendHttpJson(
            res,
            401,
            {
                success:
                    false,

                error:
                    "Usuário Firebase não encontrado"
            }
        );

        return;
    }

    if (
        !firebaseUser.email
    ) {

        sendHttpJson(
            res,
            400,
            {
                success:
                    false,

                error:
                    "A conta não possui e-mail"
            }
        );

        return;
    }

    const reservationRef =
        db.ref(
            `usernameReservations/${key}`
        );

    const usernameRef =
        db.ref(
            `usernames/${key}`
        );

    const userRef =
        db.ref(
            `users/${decodedToken.uid}`
        );

    try {

        const reservationSnapshot =
            await reservationRef.get();

        if (
            !reservationSnapshot.exists()
        ) {

            sendHttpJson(
                res,
                409,
                {
                    success:
                        false,

                    error:
                        "A reserva do nome expirou"
                }
            );

            return;
        }

        const reservation =
            reservationSnapshot.val() ||
            {};

        if (
            reservation.reservationId !==
            reservationId
        ) {

            sendHttpJson(
                res,
                409,
                {
                    success:
                        false,

                    error:
                        "A reserva do nome não pertence a esta solicitação"
                }
            );

            return;
        }

        if (
            Number(
                reservation.expiresAt ||
                    0
            ) <=
            Date.now()
        ) {

            await reservationRef.remove();

            sendHttpJson(
                res,
                409,
                {
                    success:
                        false,

                    error:
                        "A reserva do nome expirou"
                }
            );

            return;
        }

        const existing =
            await usernameRef.get();

        if (
            existing.exists()
        ) {

            await reservationRef.remove();

            sendHttpJson(
                res,
                409,
                {
                    success:
                        false,

                    error:
                        "Esse nome de usuário já está cadastrado"
                }
            );

            return;
        }

        const userData = {

            username,

            usernameKey:
                key,

            email:
                firebaseUser.email,

            status:
                "active",

            createdAt:
                Date.now()
        };

        await userRef.set(
            userData
        );

        await usernameRef.set(
            decodedToken.uid
        );

        await reservationRef.remove();

        console.log(
            `[REGISTER COMPLETE] uid=${decodedToken.uid} ` +
            `username=${username}`
        );

        sendHttpJson(
            res,
            200,
            {
                success:
                    true,

                uid:
                    decodedToken.uid,

                username
            }
        );

    } catch (error) {

        console.error(
            "[REGISTER FINALIZE]",
            error.message
        );

        sendHttpJson(
            res,
            500,
            {
                success:
                    false,

                error:
                    "Não foi possível finalizar o cadastro"
            }
        );
    }
}

// ============================================================
// RELEASE USERNAME
// ============================================================

async function handleReleaseUsername(
    req,
    res
) {

    if (
        !firebaseReady ||
        !db
    ) {

        sendHttpJson(
            res,
            503,
            {
                success:
                    false,

                error:
                    "Serviço temporariamente indisponível"
            }
        );

        return;
    }

    let body;

    try {

        body =
            await readJsonBody(
                req
            );

    } catch (error) {

        sendHttpJson(
            res,
            400,
            {
                success:
                    false,

                error:
                    error.message
            }
        );

        return;
    }

    const username =
        String(
            body.username ||
                ""
        ).trim();

    const reservationId =
        String(
            body.reservationId ||
                ""
        ).trim();

    const key =
        usernameKey(
            username
        );

    if (
        !reservationId
    ) {

        sendHttpJson(
            res,
            400,
            {
                success:
                    false,

                error:
                    "Reserva inválida"
            }
        );

        return;
    }

    try {

        const ref =
            db.ref(
                `usernameReservations/${key}`
            );

        const snapshot =
            await ref.get();

        if (
            snapshot.exists() &&
            snapshot.val()?.reservationId ===
                reservationId
        ) {

            await ref.remove();

            console.log(
                `[USERNAME RELEASE] ${key}`
            );
        }

        sendHttpJson(
            res,
            200,
            {
                success:
                    true
            }
        );

    } catch (error) {

        console.error(
            "[USERNAME RELEASE]",
            error.message
        );

        sendHttpJson(
            res,
            500,
            {
                success:
                    false,

                error:
                    "Não foi possível liberar a reserva"
            }
        );
    }
}

// ============================================================
// HTTP SERVER
// ============================================================

const httpServer =
    http.createServer(
        async (
            req,
            res
        ) => {

            setCors(
                res
            );

            if (
                req.method ===
                "OPTIONS"
            ) {

                res.writeHead(
                    204
                );

                res.end();

                return;
            }

            // ------------------------------------------------
            // HEALTH
            // ------------------------------------------------

            if (
                req.method ===
                    "GET" &&
                req.url ===
                    "/"
            ) {

                sendHttpJson(
                    res,
                    200,
                    {
                        service:
                            "Z-Link Talk",

                        status:
                            "online",

                        firebase:
                            firebaseReady,

                        timestamp:
                            Date.now()
                    }
                );

                return;
            }

            // ------------------------------------------------
            // SESSION OPEN
            // ------------------------------------------------

            if (
                req.method ===
                    "POST" &&
                req.url ===
                    "/api/session/open"
            ) {

                await handleSessionOpen(
                    req,
                    res
                );

                return;
            }

            // ------------------------------------------------
            // CHECK REGISTRATION
            // ------------------------------------------------

            if (
                req.method ===
                    "POST" &&
                req.url ===
                    "/api/check-registration"
            ) {

                await handleCheckRegistration(
                    req,
                    res
                );

                return;
            }

            // ------------------------------------------------
            // RESERVE USERNAME
            // ------------------------------------------------

            if (
                req.method ===
                    "POST" &&
                req.url ===
                    "/api/reserve-username"
            ) {

                await handleReserveUsername(
                    req,
                    res
                );

                return;
            }

            // ------------------------------------------------
            // FINALIZE REGISTRATION
            // ------------------------------------------------

            if (
                req.method ===
                    "POST" &&
                req.url ===
                    "/api/finalize-registration"
            ) {

                await handleFinalizeRegistration(
                    req,
                    res
                );

                return;
            }

            // ------------------------------------------------
            // RELEASE USERNAME
            // ------------------------------------------------

            if (
                req.method ===
                    "POST" &&
                req.url ===
                    "/api/release-username"
            ) {

                await handleReleaseUsername(
                    req,
                    res
                );

                return;
            }

            sendHttpJson(
                res,
                404,
                {
                    success:
                        false,

                    error:
                        "Endpoint não encontrado"
                }
            );
        }
    );

// ============================================================
// WEBSOCKET
// ============================================================

const wss =
    new WebSocket.WebSocketServer({
        server:
            httpServer,

        maxPayload:
            64 * 1024
    });

// ============================================================
// WEBSOCKET UTILITIES
// ============================================================

function isOpen(ws) {

    return (
        ws &&
        ws.readyState ===
            WebSocket.OPEN
    );
}

function clientList() {

    return [
        ...clients.values()
    ]
        .filter(
            isOpen
        )
        .map(
            ws => ({
                id:
                    ws.userId,

                name:
                    ws.name
            })
        );
}

function sendJson(
    ws,
    data
) {

    if (
        !isOpen(ws)
    ) {
        return;
    }

    try {

        ws.send(
            JSON.stringify(
                data
            )
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
        JSON.stringify(
            data
        );

    for (
        const ws
        of clients.values()
    ) {

        if (
            isOpen(ws) &&
            ws.userId !==
                exceptId
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
// AUDIO VALIDATION
// ============================================================

function isAudioPacket(
    buf
) {

    return (
        Buffer.isBuffer(buf) &&
        buf.length >
            AUDIO_HEADER_SIZE &&
        buf[0] ===
            AUDIO_MAGIC_0 &&
        buf[1] ===
            AUDIO_MAGIC_1 &&
        buf[2] ===
            AUDIO_VERSION
    );
}

// ============================================================
// RESET TRANSMITTER
// ============================================================

function resetTransmitterIf(
    userId
) {

    if (
        activeTransmitterId !==
        userId
    ) {

        return;
    }

    console.log(
        `[TX RESET] ${userId}`
    );

    activeTransmitterId =
        null;

    activeTransmitStartedAt =
        0;

    broadcastJson({
        type:
            "stop_tx",

        from:
            userId
    });
}

// ============================================================
// JSON CONTROL
// ============================================================

async function handleJson(
    ws,
    data
) {

    if (
        !data ||
        typeof data !==
            "object"
    ) {

        return;
    }

    // ========================================================
    // IDENTIFY
    // ========================================================

    if (
        data.type ===
        "identify"
    ) {

        const token =
            String(
                data.token ||
                    ""
            ).trim();

        const sessionId =
            String(
                data.sessionId ||
                    ""
            ).trim();

        const deviceId =
            String(
                data.deviceId ||
                    ""
            ).trim();

        if (
            !token ||
            !sessionId ||
            !deviceId
        ) {

            console.warn(
                "[IDENTIFY] dados de sessão incompletos"
            );

            try {

                ws.close(
                    4003,
                    "Authentication required"
                );

            } catch (_) {
            }

            return;
        }

        let decoded;

        try {

            decoded =
                await auth.verifyIdToken(
                    token
                );

        } catch (error) {

            console.warn(
                "[IDENTIFY] token inválido:",
                error.message
            );

            try {

                ws.close(
                    4003,
                    "Invalid authentication"
                );

            } catch (_) {
            }

            return;
        }

        const userId =
            decoded.uid;

        const validSession =
            await validateStoredSession(
                userId,
                sessionId,
                deviceId
            );

        if (
            !validSession
        ) {

            console.warn(
                `[IDENTIFY] sessão inválida uid=${userId}`
            );

            sendJson(
                ws,
                {
                    type:
                        "session_invalid"
                }
            );

            try {

                ws.close(
                    4001,
                    "Session invalid"
                );

            } catch (_) {
            }

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
                `[IDENTIFY] substituindo conexão anterior de ${userId}`
            );

            try {

                sendJson(
                    old,
                    {
                        type:
                            "session_revoked",

                        reason:
                            "Você entrou com outro dispositivo."
                    }
                );

                old.close(
                    4001,
                    "Session replaced"
                );

            } catch (_) {
            }

            resetTransmitterIf(
                userId
            );
        }

        ws.userId =
            userId;

        ws.sessionId =
            sessionId;

        ws.deviceId =
            deviceId;

        ws.name =
            String(
                data.name ||
                    decoded.name ||
                    "Anônimo"
            ).slice(
                0,
                32
            );

        clients.set(
            userId,
            ws
        );

        /*
         * Atualiza atividade da sessão.
         */
        if (
            db
        ) {

            try {

                await db
                    .ref(
                        `sessions/${userId}`
                    )
                    .update({
                        updatedAt:
                            Date.now()
                    });

            } catch (_) {
            }
        }

        console.log(
            `[IDENTIFY] ${userId} -> ${ws.name} | ` +
            `sessão=${sessionId} | ` +
            `usuários=${clients.size}`
        );

        sendJson(
            ws,
            {
                type:
                    "init",

                id:
                    userId,

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
            type:
                "user_list",

            clients:
                clientList()
        });

        return;
    }

    // ========================================================
    // IDENTIFICAÇÃO OBRIGATÓRIA
    // ========================================================

    if (
        !ws.userId
    ) {

        return;
    }

    // ========================================================
    // UPDATE NAME
    // ========================================================

    if (
        data.type ===
        "update_name"
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
            type:
                "user_update",

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
        data.type ===
        "start_tx"
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
                `tentou transmitir; ` +
                `canal ocupado por ${activeTransmitterId}`
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

        broadcastJson({
            type:
                "start_tx",

            from:
                ws.userId,

            name:
                ws.name
        });

        return;
    }

    // ========================================================
    // STOP TX
    // ========================================================

    if (
        data.type ===
        "stop_tx"
    ) {

        const duration =
            activeTransmitStartedAt >
            0
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
        data.type ===
        "ping_app"
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
    }
}

// ============================================================
// WEBSOCKET CONNECTION
// ============================================================

wss.on(
    "connection",
    ws => {

        console.log(
            "[WS] Cliente conectado"
        );

        ws.userId =
            null;

        ws.sessionId =
            null;

        ws.deviceId =
            null;

        ws.name =
            "Anônimo";

        ws.isAlive =
            true;

        ws.on(
            "pong",
            () => {

                ws.isAlive =
                    true;
            }
        );

        ws.on(
            "message",
            async (
                data,
                isBinary
            ) => {

                // ============================================
                // AUDIO
                // ============================================

                if (
                    isAudioPacket(
                        data
                    )
                ) {

                    if (
                        !ws.userId ||
                        !ws.sessionId ||
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

                    if (
                        audioPacketCount ===
                            1 ||
                        audioPacketCount %
                            100 ===
                            0
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
                // INVALID BINARY
                // ============================================

                if (
                    isBinary
                ) {

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

                } catch (_) {

                    console.warn(
                        "[JSON DROP] mensagem inválida"
                    );

                    return;
                }

                await handleJson(
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

                if (
                    !ws.userId
                ) {

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
                ws.isAlive ===
                false
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
// SERVER START
// ============================================================

httpServer.listen(
    PORT,
    "0.0.0.0",
    async () => {

        console.log(
            `Z-Link Talk Audio Server listening on ${PORT}`
        );

        await testFirebaseConnection();

        console.log(
            `[STARTUP] Firebase=${firebaseReady ? "OK" : "OFFLINE"}`
        );
    }
);

// ============================================================
// AUDIO STATS
// ============================================================

setInterval(
    () => {

        if (
            audioPacketCount >
            0
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
