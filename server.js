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
// TRANSMISSORES POR CANAL
// ============================================================
//
// channelId -> {
//   userId,
//   name,
//   startedAt,
//   audioPacketCount,
//   audioBytesRelayed
// }
//
// Cada canal possui seu próprio PTT ocupado/livre.
// Isso permite transmissões simultâneas em canais diferentes
// sem enviar áudio para usuários que não participam do canal.
//

const activeTransmitters =
    new Map();

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

/*
 * Quantidade de pacotes iniciais preservados de cada transmissão.
 *
 * 5 frames x 20 ms = aproximadamente 100 ms.
 *
 * O Android MediaCodec Opus mostrou no log que recebe normalmente
 * pacotes quando entra no meio da transmissão, mas não produz PCM.
 * Reenviar estes primeiros frames ao late-joiner fornece ao decoder
 * o mesmo início de fluxo que ele recebe quando acompanha um TX
 * desde o começo.
 */
const LATE_JOIN_BOOTSTRAP_PACKETS =
    5;

// ============================================================
// CANAIS
// ============================================================

const CHANNEL_ID_LENGTH = 8;
const MAX_CHANNEL_NAME_LENGTH = 40;
const MAX_CHANNEL_DESCRIPTION_LENGTH = 200;

// Avatar pequeno armazenado como data URL (JPEG/PNG/WebP) no Realtime Database.
// Mantemos um limite conservador para não deixar o banco crescer sem controle.
const MAX_AVATAR_LENGTH = 80_000;

function sanitizeChannelName(value) {
    return String(value || "")
        .trim()
        .replace(/\s+/g, " ")
        .slice(0, MAX_CHANNEL_NAME_LENGTH);
}

function sanitizeChannelDescription(value) {
    return String(value || "")
        .trim()
        .slice(0, MAX_CHANNEL_DESCRIPTION_LENGTH);
}

function normalizeChannelType(value) {
    return value === "private" ? "private" : "public";
}

function sanitizeAvatar(value) {
    const text = String(value || "").trim();

    if (!text) {
        return "";
    }

    // Imagem enviada pelo app ao criar o canal.
    if (/^data:image\/(jpeg|jpg|png|webp);base64,[A-Za-z0-9+/=\r\n]+$/i.test(text)) {
        return text.length <= MAX_AVATAR_LENGTH ? text : "";
    }

    // Também aceitamos URL HTTPS para compatibilidade futura com avatar de usuário.
    if (/^https:\/\/[^\s]+$/i.test(text) && text.length <= 2048) {
        return text;
    }

    return "";
}

function isValidChannelId(value) {
    return /^[0-9]{8}$/.test(String(value || ""));
}

async function generateUniqueChannelId() {
    for (let attempt = 0; attempt < 20; attempt++) {
        const id = String(
            crypto.randomInt(0, 100_000_000)
        ).padStart(CHANNEL_ID_LENGTH, "0");

        const snapshot = await db
            .ref(`channels/${id}`)
            .get();

        if (!snapshot.exists()) {
            return id;
        }
    }

    throw new Error("Não foi possível gerar um ID de canal disponível");
}

async function getUserProfile(uid) {
    const snapshot = await db
        .ref(`users/${uid}`)
        .get();

    return snapshot.exists() ? snapshot.val() || {} : {};
}

function getChannelModeratorUids(channel) {
    /*
     * moderators = estrutura nova.
     * admins = estrutura antiga, lida apenas para compatibilidade/migração.
     */
    const moderators =
        channel &&
        typeof channel.moderators === "object" &&
        channel.moderators
            ? channel.moderators
            : {};

    const legacyAdmins =
        channel &&
        typeof channel.admins === "object" &&
        channel.admins
            ? channel.admins
            : {};

    return [
        ...new Set(
            [
                ...Object.keys(moderators),
                ...Object.keys(legacyAdmins)
            ]
                .map(uid => String(uid || "").trim())
                .filter(Boolean)
        )
    ];
}

function getChannelAdminUids(channel) {
    /*
     * Compatibilidade com clientes antigos.
     * A partir desta versão estes UIDs representam MODERADORES.
     */
    return getChannelModeratorUids(channel);
}

function isChannelOwnerUser(channel, uid) {
    return String(channel?.ownerUid || "") === String(uid || "");
}

function isChannelModerator(channel, uid) {
    const normalizedUid = String(uid || "");

    if (!normalizedUid) {
        return false;
    }

    const inModerators =
        !!channel?.moderators &&
        Object.prototype.hasOwnProperty.call(
            channel.moderators,
            normalizedUid
        );

    const inLegacyAdmins =
        !!channel?.admins &&
        Object.prototype.hasOwnProperty.call(
            channel.admins,
            normalizedUid
        );

    return inModerators || inLegacyAdmins;
}

function isChannelDelegatedAdmin(channel, uid) {
    /*
     * Alias temporário para trechos antigos do servidor.
     * Semântica nova: antigo administrador delegado = Moderador.
     */
    return isChannelModerator(channel, uid);
}

function canModerateChannel(channel, uid) {
    return (
        isChannelOwnerUser(channel, uid) ||
        isChannelModerator(channel, uid)
    );
}

function updateInMemoryChannelModerators(channelId, channel) {
    const moderatorUids =
        getChannelModeratorUids(channel);

    for (const ws of clients.values()) {
        const item = Array.isArray(ws.channels)
            ? ws.channels.find(
                entry =>
                    String(entry.id) === String(channelId)
            )
            : null;

        if (item) {
            item.moderatorUids =
                moderatorUids;

            /*
             * Compatibilidade com Android antigo.
             */
            item.adminUids =
                moderatorUids;
        }
    }
}

function updateInMemoryChannelAdmins(channelId, channel) {
    updateInMemoryChannelModerators(
        channelId,
        channel
    );
}

async function getUserChannels(uid) {
    const membershipSnapshot = await db
        .ref(`users/${uid}/channels`)
        .get();

    const memberships = membershipSnapshot.exists()
        ? membershipSnapshot.val() || {}
        : {};

    const result = [];

    for (const [channelId, membership] of Object.entries(memberships)) {
        const channelSnapshot = await db
            .ref(`channels/${channelId}`)
            .get();

        if (!channelSnapshot.exists()) {
            continue;
        }

        const channel = channelSnapshot.val() || {};
        const blocked =
            !!channel.blockedUsers &&
            Object.prototype.hasOwnProperty.call(
                channel.blockedUsers,
                uid
            );

        result.push({
            id: channelId,
            name: channel.name || "Canal",
            description: channel.description || "",
            type: channel.type || "public",
            ownerUid: channel.ownerUid || "",
            moderatorUids: getChannelModeratorUids(channel),

            // Compatibilidade com versões antigas do Android.
            adminUids: getChannelModeratorUids(channel),

            avatar: sanitizeAvatar(channel.avatar),
            blocked,
            enabled: !blocked && membership?.enabled !== false,
            addedAt: Number(membership?.addedAt || 0)
        });
    }

    result.sort((a, b) => b.addedAt - a.addedAt);

    const userProfile = await getUserProfile(uid);

    return {
        channels: result,
        defaultChannelId: userProfile.defaultChannelId || null
    };
}

// ============================================================
// ESTADO DE CANAIS PARA O WEBSOCKET
// ============================================================

async function loadClientChannelState(uid) {
    const data = await getUserChannels(uid);

    const enabledChannelIds = new Set(
        data.channels
            .filter(channel => channel.enabled === true)
            .map(channel => String(channel.id))
    );

    const blockedChannelIds = new Set(
        data.channels
            .filter(channel => channel.blocked === true)
            .map(channel => String(channel.id))
    );

    let defaultChannelId = data.defaultChannelId
        ? String(data.defaultChannelId)
        : null;

    // Canal padrão desligado não pode transmitir.
    if (
        defaultChannelId &&
        !enabledChannelIds.has(defaultChannelId)
    ) {
        defaultChannelId = null;
    }

    return {
        channels: data.channels,
        enabledChannelIds,
        blockedChannelIds,
        defaultChannelId
    };
}

async function refreshConnectedClientChannels(uid) {
    const ws = clients.get(uid);

    if (!ws || !isOpen(ws)) {
        return;
    }

    try {
        const state = await loadClientChannelState(uid);

        const previousTxChannelId = ws.txChannelId || null;

        ws.channels = state.channels;
        ws.enabledChannelIds = state.enabledChannelIds;
        ws.blockedChannelIds = state.blockedChannelIds;
        ws.defaultChannelId = state.defaultChannelId;

        // O canal aberto na interface é independente do canal padrão.
        // Se ele deixou de existir/ficou desligado, voltamos para o padrão
        // ativo (quando existir).
        if (
            ws.activeChannelId &&
            !ws.enabledChannelIds.has(ws.activeChannelId)
        ) {
            ws.activeChannelId =
                ws.defaultChannelId &&
                ws.enabledChannelIds.has(ws.defaultChannelId)
                    ? ws.defaultChannelId
                    : null;
        }

        // A transmissão só é encerrada se o canal usado deixou de estar
        // habilitado ou deixou de ser o canal atualmente selecionado.
        if (
            previousTxChannelId &&
            (
                !ws.enabledChannelIds.has(previousTxChannelId) ||
                ws.activeChannelId !== previousTxChannelId
            )
        ) {
            resetTransmitterIf(uid);

            sendJson(ws, {
                type: "tx_denied",
                code: "CHANNEL_CHANGED",
                message: "O canal de transmissão foi alterado."
            });
        }

        sendJson(ws, buildChannelStateMessage(ws));
        broadcastUserLists();

    } catch (error) {
        console.error(
            `[CHANNEL WS REFRESH] uid=${uid} ${error.message}`
        );
    }
}

async function handleCreateChannel(req, res) {
    if (!firebaseReady || !db || !auth) {
        sendHttpJson(res, 503, {
            success: false,
            error: "Serviço temporariamente indisponível"
        });
        return;
    }

    let decoded;
    try {
        decoded = await verifyBearerToken(req);
    } catch (_) {
        sendHttpJson(res, 401, {
            success: false,
            error: "Sessão inválida ou expirada"
        });
        return;
    }

    let body;
    try {
        body = await readJsonBody(req);
    } catch (error) {
        sendHttpJson(res, 400, {
            success: false,
            error: error.message
        });
        return;
    }

    const name = sanitizeChannelName(body.name);
    const description = sanitizeChannelDescription(body.description);
    const type = normalizeChannelType(body.type);
    const avatar = sanitizeAvatar(body.avatar);

    if (name.length < 3) {
        sendHttpJson(res, 400, {
            success: false,
            error: "O nome do canal deve ter pelo menos 3 caracteres"
        });
        return;
    }

    try {
        const channelId = await generateUniqueChannelId();
        const now = Date.now();
        const updates = {};

        updates[`channels/${channelId}`] = {
            name,
            description,
            type,
            ownerUid: decoded.uid,
            avatar,
            createdAt: now,
            updatedAt: now
        };

        // Índice somente para canais públicos.
        if (type === "public") {
            updates[`publicChannels/${channelId}`] = {
                name,
                description,
                avatar,
                createdAt: now
            };
        }

        updates[`users/${decoded.uid}/channels/${channelId}`] = {
            enabled: true,
            addedAt: now
        };

        const userSnapshot = await db
            .ref(`users/${decoded.uid}`)
            .get();

        const userProfile = userSnapshot.exists()
            ? userSnapshot.val() || {}
            : {};

        if (!userProfile.defaultChannelId) {
            updates[`users/${decoded.uid}/defaultChannelId`] = channelId;
        }

        await db.ref().update(updates);

        await refreshConnectedClientChannels(decoded.uid);

        console.log(
            `[CHANNEL CREATE] uid=${decoded.uid} id=${channelId} type=${type} name=${name}`
        );

        sendHttpJson(res, 200, {
            success: true,
            channel: {
                id: channelId,
                name,
                description,
                type,
                ownerUid: decoded.uid,
                avatar,
                enabled: true,
                isDefault: !userProfile.defaultChannelId
            }
        });
    } catch (error) {
        console.error("[CHANNEL CREATE]", error.message);
        sendHttpJson(res, 500, {
            success: false,
            error: "Não foi possível criar o canal"
        });
    }
}

async function handleListUserChannels(req, res) {
    if (!firebaseReady || !db || !auth) {
        sendHttpJson(res, 503, {
            success: false,
            error: "Serviço temporariamente indisponível"
        });
        return;
    }

    let decoded;
    try {
        decoded = await verifyBearerToken(req);
    } catch (_) {
        sendHttpJson(res, 401, {
            success: false,
            error: "Sessão inválida ou expirada"
        });
        return;
    }

    try {
        const data = await getUserChannels(decoded.uid);
        sendHttpJson(res, 200, {
            success: true,
            channels: data.channels,
            defaultChannelId: data.defaultChannelId
        });
    } catch (error) {
        console.error("[CHANNEL LIST]", error.message);
        sendHttpJson(res, 500, {
            success: false,
            error: "Não foi possível carregar os canais"
        });
    }
}

async function handleSearchPublicChannels(req, res) {
    if (!firebaseReady || !db || !auth) {
        sendHttpJson(res, 503, {
            success: false,
            error: "Serviço temporariamente indisponível"
        });
        return;
    }

    let decoded;
    try {
        decoded = await verifyBearerToken(req);
    } catch (_) {
        sendHttpJson(res, 401, {
            success: false,
            error: "Sessão inválida ou expirada"
        });
        return;
    }

    const url = new URL(
        req.url,
        `http://${req.headers.host || "localhost"}`
    );

    const query = String(
        url.searchParams.get("q") || ""
    )
        .trim()
        .toLowerCase();

    if (query.length < 2) {
        sendHttpJson(res, 400, {
            success: false,
            error: "Digite pelo menos 2 caracteres para pesquisar"
        });
        return;
    }

    try {
        const snapshot = await db
            .ref("publicChannels")
            .get();

        const all = snapshot.exists()
            ? snapshot.val() || {}
            : {};

        const ownChannels = await db
            .ref(`users/${decoded.uid}/channels`)
            .get();

        const own = ownChannels.exists()
            ? ownChannels.val() || {}
            : {};

        const results = Object.entries(all)
            .filter(([id, channel]) => {
                const normalizedName = String(channel?.name || "")
                    .toLowerCase();

                return (
                    normalizedName.includes(query) &&
                    !Object.prototype.hasOwnProperty.call(own, id)
                );
            })
            .slice(0, 50)
            .map(([id, channel]) => ({
                id,
                name: channel?.name || "Canal",
                description: channel?.description || "",
                avatar: sanitizeAvatar(channel?.avatar),
                type: "public"
            }));

        sendHttpJson(res, 200, {
            success: true,
            channels: results
        });
    } catch (error) {
        console.error("[CHANNEL SEARCH]", error.message);
        sendHttpJson(res, 500, {
            success: false,
            error: "Não foi possível pesquisar os canais"
        });
    }
}

async function handleAddChannel(req, res) {
    if (!firebaseReady || !db || !auth) {
        sendHttpJson(res, 503, {
            success: false,
            error: "Serviço temporariamente indisponível"
        });
        return;
    }

    let decoded;
    try {
        decoded = await verifyBearerToken(req);
    } catch (_) {
        sendHttpJson(res, 401, {
            success: false,
            error: "Sessão inválida ou expirada"
        });
        return;
    }

    let body;
    try {
        body = await readJsonBody(req);
    } catch (error) {
        sendHttpJson(res, 400, {
            success: false,
            error: error.message
        });
        return;
    }

    const channelId = String(
        body.channelId || ""
    ).trim();

    if (!isValidChannelId(channelId)) {
        sendHttpJson(res, 400, {
            success: false,
            error: "ID de canal inválido"
        });
        return;
    }

    try {
        const channelSnapshot = await db
            .ref(`channels/${channelId}`)
            .get();

        if (!channelSnapshot.exists()) {
            sendHttpJson(res, 404, {
                success: false,
                error: "Canal não encontrado"
            });
            return;
        }

        const channel = channelSnapshot.val() || {};

        if (
            channel.blockedUsers &&
            Object.prototype.hasOwnProperty.call(
                channel.blockedUsers,
                decoded.uid
            )
        ) {
            sendHttpJson(res, 403, {
                success: false,
                code: "CHANNEL_BLOCKED",
                error: "Você não pode entrar neste canal, pois foi bloqueado nele."
            });
            return;
        }

        const membershipRef = db
            .ref(`users/${decoded.uid}/channels/${channelId}`);

        const existingMembership = await membershipRef.get();

        if (existingMembership.exists()) {
            sendHttpJson(res, 409, {
                success: false,
                error: "Este canal já foi adicionado"
            });
            return;
        }

        const now = Date.now();
        await membershipRef.set({
            enabled: true,
            addedAt: now
        });

        const userSnapshot = await db
            .ref(`users/${decoded.uid}`)
            .get();

        const userProfile = userSnapshot.exists()
            ? userSnapshot.val() || {}
            : {};

        let isDefault = false;

        if (!userProfile.defaultChannelId) {
            await db
                .ref(`users/${decoded.uid}/defaultChannelId`)
                .set(channelId);
            isDefault = true;
        }

        await refreshConnectedClientChannels(decoded.uid);

        console.log(
            `[CHANNEL ADD] uid=${decoded.uid} id=${channelId}`
        );

        sendHttpJson(res, 200, {
            success: true,
            channel: {
                id: channelId,
                name: channel.name || "Canal",
                description: channel.description || "",
                type: channel.type || "private",
                ownerUid: channel.ownerUid || "",
                avatar: sanitizeAvatar(channel.avatar),
                enabled: true,
                isDefault
            }
        });
    } catch (error) {
        console.error("[CHANNEL ADD]", error.message);
        sendHttpJson(res, 500, {
            success: false,
            error: "Não foi possível adicionar o canal"
        });
    }
}


async function handleGetUserMe(req, res) {
    if (!firebaseReady || !db || !auth) {
        sendHttpJson(res, 503, {
            success: false,
            error: "Serviço temporariamente indisponível"
        });
        return;
    }

    let decoded;
    try {
        decoded = await verifyBearerToken(req);
    } catch (_) {
        sendHttpJson(res, 401, {
            success: false,
            error: "Sessão inválida ou expirada"
        });
        return;
    }

    try {
        const profile = await getUserProfile(decoded.uid);
        sendHttpJson(res, 200, {
            success: true,
            uid: decoded.uid,
            username: profile.username || decoded.name || "",
            avatar: sanitizeAvatar(
                profile.avatar ||
                profile.avatarData ||
                profile.photoUrl ||
                decoded.picture ||
                ""
            )
        });
    } catch (error) {
        console.error("[USER ME]", error.message);
        sendHttpJson(res, 500, {
            success: false,
            error: "Não foi possível carregar o perfil"
        });
    }
}

async function handleSetUserAvatar(req, res) {
    if (!firebaseReady || !db || !auth) {
        sendHttpJson(res, 503, {
            success: false,
            error: "Serviço temporariamente indisponível"
        });
        return;
    }

    let decoded;
    try {
        decoded = await verifyBearerToken(req);
    } catch (_) {
        sendHttpJson(res, 401, {
            success: false,
            error: "Sessão inválida ou expirada"
        });
        return;
    }

    let body;
    try {
        body = await readJsonBody(req);
    } catch (error) {
        sendHttpJson(res, 400, {
            success: false,
            error: error.message
        });
        return;
    }

    const rawAvatar = String(body.avatar || "").trim();
    const avatar = sanitizeAvatar(rawAvatar);

    if (rawAvatar && !avatar) {
        sendHttpJson(res, 400, {
            success: false,
            error: "Imagem inválida ou muito grande"
        });
        return;
    }

    try {
        await db
            .ref(`users/${decoded.uid}/avatar`)
            .set(avatar || null);

        const ws = clients.get(decoded.uid);
        if (ws && isOpen(ws)) {
            ws.avatar = avatar;

            for (const state of activeTransmitters.values()) {
                if (state.userId === decoded.uid) {
                    state.avatar = avatar;
                }
            }

            // Atualiza imediatamente todos que compartilham algum canal com este usuário.
            for (const peer of clients.values()) {
                if (!isOpen(peer) || !shareAnyEnabledChannel(peer, ws)) {
                    continue;
                }

                sendJson(peer, {
                    type: "user_update",
                    id: decoded.uid,
                    name: ws.name || "Anônimo",
                    avatar
                });
            }
        }

        broadcastUserLists();

        sendHttpJson(res, 200, {
            success: true,
            avatar
        });
    } catch (error) {
        console.error("[USER AVATAR]", error.message);
        sendHttpJson(res, 500, {
            success: false,
            error: "Não foi possível alterar a imagem do perfil"
        });
    }
}

async function handleSetChannelAvatar(req, res) {
    if (!firebaseReady || !db || !auth) {
        sendHttpJson(res, 503, {
            success: false,
            error: "Serviço temporariamente indisponível"
        });
        return;
    }

    let decoded;
    try {
        decoded = await verifyBearerToken(req);
    } catch (_) {
        sendHttpJson(res, 401, {
            success: false,
            error: "Sessão inválida ou expirada"
        });
        return;
    }

    let body;
    try {
        body = await readJsonBody(req);
    } catch (error) {
        sendHttpJson(res, 400, {
            success: false,
            error: error.message
        });
        return;
    }

    const channelId = String(body.channelId || "").trim();
    const rawAvatar = String(body.avatar || "").trim();
    const avatar = sanitizeAvatar(rawAvatar);

    if (!isValidChannelId(channelId)) {
        sendHttpJson(res, 400, {
            success: false,
            error: "ID de canal inválido"
        });
        return;
    }

    if (rawAvatar && !avatar) {
        sendHttpJson(res, 400, {
            success: false,
            error: "Imagem inválida ou muito grande"
        });
        return;
    }

    try {
        const channelRef = db.ref(`channels/${channelId}`);
        const snapshot = await channelRef.get();

        if (!snapshot.exists()) {
            sendHttpJson(res, 404, {
                success: false,
                error: "Canal não encontrado"
            });
            return;
        }

        const channel = snapshot.val() || {};
        if (String(channel.ownerUid || "") !== decoded.uid) {
            sendHttpJson(res, 403, {
                success: false,
                error: "Somente o criador do canal pode alterar esta imagem"
            });
            return;
        }

        const updates = {};
        updates[`channels/${channelId}/avatar`] = avatar || null;
        updates[`channels/${channelId}/updatedAt`] = Date.now();

        if ((channel.type || "public") === "public") {
            updates[`publicChannels/${channelId}/avatar`] = avatar || null;
        }

        await db.ref().update(updates);

        // Atualiza o estado em memória e avisa clientes já conectados.
        for (const ws of clients.values()) {
            const item = (ws.channels || [])
                .find(entry => String(entry.id) === channelId);

            if (!item) {
                continue;
            }

            item.avatar = avatar;
            if (isOpen(ws)) {
                sendJson(ws, buildChannelStateMessage(ws));
            }
        }

        sendHttpJson(res, 200, {
            success: true,
            channelId,
            avatar
        });
    } catch (error) {
        console.error("[CHANNEL AVATAR]", error.message);
        sendHttpJson(res, 500, {
            success: false,
            error: "Não foi possível alterar a imagem do canal"
        });
    }
}

async function handleDeleteChannel(req, res) {
    if (!firebaseReady || !db || !auth) {
        sendHttpJson(res, 503, {
            success: false,
            error: "Serviço temporariamente indisponível"
        });
        return;
    }

    let decoded;
    try {
        decoded = await verifyBearerToken(req);
    } catch (_) {
        sendHttpJson(res, 401, {
            success: false,
            error: "Sessão inválida ou expirada"
        });
        return;
    }

    let body;
    try {
        body = await readJsonBody(req);
    } catch (error) {
        sendHttpJson(res, 400, {
            success: false,
            error: error.message
        });
        return;
    }

    const channelId =
        String(
            body.channelId ||
                ""
        ).trim();

    if (!isValidChannelId(channelId)) {
        sendHttpJson(res, 400, {
            success: false,
            error: "ID de canal inválido"
        });
        return;
    }

    try {
        const channelRef =
            db.ref(
                `channels/${channelId}`
            );

        const channelSnapshot =
            await channelRef.get();

        if (!channelSnapshot.exists()) {
            sendHttpJson(res, 404, {
                success: false,
                error: "Canal não encontrado"
            });
            return;
        }

        const channel =
            channelSnapshot.val() ||
            {};

        if (
            String(
                channel.ownerUid ||
                    ""
            ) !== decoded.uid
        ) {
            sendHttpJson(res, 403, {
                success: false,
                error: "Somente o criador do canal pode deletá-lo"
            });
            return;
        }

        /*
         * Se houver transmissão neste canal, encerra primeiro.
         * Isso usa o estado em memória existente sem alterar o
         * protocolo de áudio nem a lógica dos outros canais.
         */
        const activeState =
            activeTransmitters.get(
                channelId
            );

        if (activeState) {
            activeTransmitters.delete(
                channelId
            );

            const txClient =
                clients.get(
                    activeState.userId
                );

            if (
                txClient &&
                txClient.txChannelId === channelId
            ) {
                txClient.txChannelId =
                    null;
            }

            broadcastJsonToChannel(
                channelId,
                {
                    type:
                        "stop_tx",

                    from:
                        activeState.userId,

                    channelId
                }
            );
        }

        /*
         * Remove o canal da coleção principal, da busca pública
         * e da lista de todos os usuários que o adicionaram.
         * Também limpa o canal padrão de quem ainda apontava para ele.
         */
        const usersSnapshot =
            await db
                .ref("users")
                .get();

        const allUsers =
            usersSnapshot.exists()
                ? usersSnapshot.val() || {}
                : {};

        const updates = {
            [`channels/${channelId}`]:
                null,

            [`publicChannels/${channelId}`]:
                null
        };

        const affectedUids =
            new Set();

        for (
            const [
                uid,
                profileValue
            ]
            of Object.entries(
                allUsers
            )
        ) {
            const profile =
                profileValue ||
                {};

            const memberships =
                profile.channels ||
                {};

            if (
                Object.prototype.hasOwnProperty.call(
                    memberships,
                    channelId
                )
            ) {
                updates[
                    `users/${uid}/channels/${channelId}`
                ] = null;

                affectedUids.add(
                    uid
                );
            }

            if (
                String(
                    profile.defaultChannelId ||
                        ""
                ) === channelId
            ) {
                updates[
                    `users/${uid}/defaultChannelId`
                ] = null;

                affectedUids.add(
                    uid
                );
            }
        }

        affectedUids.add(
            decoded.uid
        );

        await db
            .ref()
            .update(
                updates
            );

        /*
         * Sincroniza imediatamente os WebSockets dos usuários
         * afetados. Assim o canal desaparece sem precisar reiniciar
         * o aplicativo ou desligar o Wi-Fi.
         */
        for (
            const uid
            of affectedUids
        ) {
            await refreshConnectedClientChannels(
                uid
            );
        }

        broadcastUserLists();

        console.log(
            `[CHANNEL DELETE] uid=${decoded.uid} id=${channelId}`
        );

        sendHttpJson(res, 200, {
            success: true,
            channelId
        });

    } catch (error) {
        console.error(
            "[CHANNEL DELETE]",
            error.message
        );

        sendHttpJson(res, 500, {
            success: false,
            error: "Não foi possível deletar o canal"
        });
    }
}

async function handleRemoveChannel(req, res) {
    if (!firebaseReady || !db || !auth) {
        sendHttpJson(res, 503, {
            success: false,
            error: "Serviço temporariamente indisponível"
        });
        return;
    }

    let decoded;
    try {
        decoded = await verifyBearerToken(req);
    } catch (_) {
        sendHttpJson(res, 401, {
            success: false,
            error: "Sessão inválida ou expirada"
        });
        return;
    }

    let body;
    try {
        body = await readJsonBody(req);
    } catch (error) {
        sendHttpJson(res, 400, {
            success: false,
            error: error.message
        });
        return;
    }

    const channelId = String(
        body.channelId || ""
    ).trim();

    if (!isValidChannelId(channelId)) {
        sendHttpJson(res, 400, {
            success: false,
            error: "ID de canal inválido"
        });
        return;
    }

    try {
        const channelSnapshot = await db
            .ref(`channels/${channelId}`)
            .get();

        if (!channelSnapshot.exists()) {
            sendHttpJson(res, 404, {
                success: false,
                error: "Canal não encontrado"
            });
            return;
        }

        const channel = channelSnapshot.val() || {};

        // O criador deve excluir o canal permanentemente; não deixamos
        // o canal ficar sem seu responsável principal por engano.
        if (String(channel.ownerUid || "") === decoded.uid) {
            sendHttpJson(res, 403, {
                success: false,
                error: "O criador do canal deve usar a opção Excluir canal"
            });
            return;
        }

        const membershipRef = db.ref(
            `users/${decoded.uid}/channels/${channelId}`
        );
        const membershipSnapshot = await membershipRef.get();

        if (!membershipSnapshot.exists()) {
            sendHttpJson(res, 404, {
                success: false,
                error: "Canal não está na sua lista"
            });
            return;
        }

        const profileSnapshot = await db
            .ref(`users/${decoded.uid}`)
            .get();
        const profile = profileSnapshot.exists()
            ? profileSnapshot.val() || {}
            : {};

        // Se este usuário estiver transmitindo neste canal, encerra antes.
        const activeState = activeTransmitters.get(channelId);
        if (activeState && activeState.userId === decoded.uid) {
            resetTransmitterIf(decoded.uid);
        }

        const wasModerator =
            isChannelModerator(channel, decoded.uid);

        const updates = {
            [`users/${decoded.uid}/channels/${channelId}`]: null
        };

        if (wasModerator) {
            updates[`channels/${channelId}/moderators/${decoded.uid}`] = null;
            updates[`channels/${channelId}/admins/${decoded.uid}`] = null;
        }

        if (String(profile.defaultChannelId || "") === channelId) {
            updates[`users/${decoded.uid}/defaultChannelId`] = null;
        }

        await db.ref().update(updates);

        if (wasModerator) {
            const refreshedChannelSnapshot = await db
                .ref(`channels/${channelId}`)
                .get();
            if (refreshedChannelSnapshot.exists()) {
                updateInMemoryChannelAdmins(
                    channelId,
                    refreshedChannelSnapshot.val() || {}
                );
            }
        }

        await refreshConnectedClientChannels(decoded.uid);
        broadcastUserLists();

        console.log(
            `[CHANNEL REMOVE] uid=${decoded.uid} id=${channelId}`
        );

        sendHttpJson(res, 200, {
            success: true,
            channelId
        });

    } catch (error) {
        console.error(
            "[CHANNEL REMOVE]",
            error.message
        );

        sendHttpJson(res, 500, {
            success: false,
            error: "Não foi possível remover o canal da sua lista"
        });
    }
}


// ============================================================
// ADMINISTRAÇÃO DE USUÁRIOS DO CANAL
// ============================================================

async function getOwnedChannelOrRespond(res, ownerUid, channelId) {
    if (!isValidChannelId(channelId)) {
        sendHttpJson(res, 400, {
            success: false,
            error: "ID de canal inválido"
        });
        return null;
    }

    const snapshot = await db
        .ref(`channels/${channelId}`)
        .get();

    if (!snapshot.exists()) {
        sendHttpJson(res, 404, {
            success: false,
            error: "Canal não encontrado"
        });
        return null;
    }

    const channel = snapshot.val() || {};

    if (String(channel.ownerUid || "") !== ownerUid) {
        sendHttpJson(res, 403, {
            success: false,
            error: "Somente o Administrador do canal pode executar esta ação"
        });
        return null;
    }

    return channel;
}

async function getManagedChannelOrRespond(res, actorUid, channelId) {
    if (!isValidChannelId(channelId)) {
        sendHttpJson(res, 400, {
            success: false,
            error: "ID de canal inválido"
        });
        return null;
    }

    const snapshot = await db
        .ref(`channels/${channelId}`)
        .get();

    if (!snapshot.exists()) {
        sendHttpJson(res, 404, {
            success: false,
            error: "Canal não encontrado"
        });
        return null;
    }

    const channel = snapshot.val() || {};

    if (!canModerateChannel(channel, actorUid)) {
        sendHttpJson(res, 403, {
            success: false,
            error: "Você não tem permissão para moderar usuários deste canal"
        });
        return null;
    }

    return channel;
}

async function handleDisconnectChannelUser(req, res) {
    if (!firebaseReady || !db || !auth) {
        sendHttpJson(res, 503, {
            success: false,
            error: "Serviço temporariamente indisponível"
        });
        return;
    }

    let decoded;
    try {
        decoded = await verifyBearerToken(req);
    } catch (_) {
        sendHttpJson(res, 401, {
            success: false,
            error: "Sessão inválida ou expirada"
        });
        return;
    }

    let body;
    try {
        body = await readJsonBody(req);
    } catch (error) {
        sendHttpJson(res, 400, {
            success: false,
            error: error.message
        });
        return;
    }

    const channelId = String(body.channelId || "").trim();
    const targetUid = String(body.userId || "").trim();

    if (!targetUid) {
        sendHttpJson(res, 400, {
            success: false,
            error: "Usuário inválido"
        });
        return;
    }

    if (targetUid === decoded.uid) {
        sendHttpJson(res, 400, {
            success: false,
            error: "O administrador não pode desconectar a si mesmo"
        });
        return;
    }

    try {
        const channel = await getOwnedChannelOrRespond(
            res,
            decoded.uid,
            channelId
        );
        if (!channel) return;

        const membershipRef = db.ref(
            `users/${targetUid}/channels/${channelId}`
        );
        const membershipSnapshot = await membershipRef.get();

        if (!membershipSnapshot.exists()) {
            sendHttpJson(res, 404, {
                success: false,
                error: "Este usuário não participa do canal"
            });
            return;
        }

        const membership = membershipSnapshot.val() || {};
        const targetProfile = await getUserProfile(targetUid);
        const updates = {};

        updates[`users/${targetUid}/channels/${channelId}/enabled`] = false;
        updates[`users/${targetUid}/channels/${channelId}/addedAt`] =
            Number(membership.addedAt || Date.now());

        if (String(targetProfile.defaultChannelId || "") === channelId) {
            updates[`users/${targetUid}/defaultChannelId`] = null;
        }

        await db.ref().update(updates);

        resetTransmitterIf(targetUid);

        const targetWs = clients.get(targetUid);
        if (
            targetWs &&
            isOpen(targetWs) &&
            targetWs.activeChannelId === channelId
        ) {
            sendJson(targetWs, {
                type: "channel_disconnected",
                channelId,
                message: "Você foi desconectado deste canal pelo Administrador."
            });
        }

        await refreshConnectedClientChannels(targetUid);
        broadcastUserLists();

        console.log(
            `[CHANNEL DISCONNECT USER] admin=${decoded.uid} ` +
            `target=${targetUid} channel=${channelId}`
        );

        sendHttpJson(res, 200, {
            success: true,
            channelId,
            userId: targetUid
        });

    } catch (error) {
        console.error("[CHANNEL DISCONNECT USER]", error.message);
        sendHttpJson(res, 500, {
            success: false,
            error: "Não foi possível desconectar o usuário"
        });
    }
}

async function handleBlockChannelUser(req, res) {
    if (!firebaseReady || !db || !auth) {
        sendHttpJson(res, 503, {
            success: false,
            error: "Serviço temporariamente indisponível"
        });
        return;
    }

    let decoded;
    try {
        decoded = await verifyBearerToken(req);
    } catch (_) {
        sendHttpJson(res, 401, {
            success: false,
            error: "Sessão inválida ou expirada"
        });
        return;
    }

    let body;
    try {
        body = await readJsonBody(req);
    } catch (error) {
        sendHttpJson(res, 400, {
            success: false,
            error: error.message
        });
        return;
    }

    const channelId = String(body.channelId || "").trim();
    const targetUid = String(body.userId || "").trim();

    if (!targetUid) {
        sendHttpJson(res, 400, {
            success: false,
            error: "Usuário inválido"
        });
        return;
    }

    if (targetUid === decoded.uid) {
        sendHttpJson(res, 400, {
            success: false,
            error: "O administrador não pode bloquear a si mesmo"
        });
        return;
    }

    try {
        const channel = await getManagedChannelOrRespond(
            res,
            decoded.uid,
            channelId
        );
        if (!channel) return;

        const actorIsOwner = isChannelOwnerUser(channel, decoded.uid);
        const targetIsOwner = isChannelOwnerUser(channel, targetUid);
        const targetIsModerator = isChannelModerator(channel, targetUid);

        if (targetIsOwner) {
            sendHttpJson(res, 403, {
                success: false,
                error: "O criador do canal não pode ser bloqueado"
            });
            return;
        }

        if (!actorIsOwner && targetIsModerator) {
            sendHttpJson(res, 403, {
                success: false,
                error: "Um Moderador não pode bloquear outro Moderador"
            });
            return;
        }

        const membershipRef = db.ref(
            `users/${targetUid}/channels/${channelId}`
        );
        const membershipSnapshot = await membershipRef.get();

        if (!membershipSnapshot.exists()) {
            sendHttpJson(res, 404, {
                success: false,
                error: "Este usuário não participa do canal"
            });
            return;
        }

        const membership = membershipSnapshot.val() || {};
        const targetProfile = await getUserProfile(targetUid);
        const now = Date.now();
        const updates = {};

        updates[`channels/${channelId}/blockedUsers/${targetUid}`] = {
            blockedAt: now,
            blockedBy: decoded.uid
        };

        if (actorIsOwner && targetIsModerator) {
            updates[`channels/${channelId}/moderators/${targetUid}`] = null;

            // Remove também eventual função antiga.
            updates[`channels/${channelId}/admins/${targetUid}`] = null;
        }

        updates[`channels/${channelId}/updatedAt`] = now;
        updates[`users/${targetUid}/channels/${channelId}/enabled`] = false;
        updates[`users/${targetUid}/channels/${channelId}/addedAt`] =
            Number(membership.addedAt || now);

        if (String(targetProfile.defaultChannelId || "") === channelId) {
            updates[`users/${targetUid}/defaultChannelId`] = null;
        }

        await db.ref().update(updates);

        if (actorIsOwner && targetIsModerator) {
            const refreshedChannelSnapshot = await db
                .ref(`channels/${channelId}`)
                .get();
            if (refreshedChannelSnapshot.exists()) {
                updateInMemoryChannelAdmins(
                    channelId,
                    refreshedChannelSnapshot.val() || {}
                );
            }
        }

        resetTransmitterIf(targetUid);

        const targetWs = clients.get(targetUid);
        if (
            targetWs &&
            isOpen(targetWs) &&
            targetWs.activeChannelId === channelId
        ) {
            sendJson(targetWs, {
                type: "channel_blocked",
                channelId,
                message: "Você foi bloqueado neste canal pela Moderação."
            });
        }

        await refreshConnectedClientChannels(targetUid);
        broadcastUserLists();

        console.log(
            `[CHANNEL BLOCK USER] admin=${decoded.uid} ` +
            `target=${targetUid} channel=${channelId}`
        );

        sendHttpJson(res, 200, {
            success: true,
            channelId,
            userId: targetUid
        });

    } catch (error) {
        console.error("[CHANNEL BLOCK USER]", error.message);
        sendHttpJson(res, 500, {
            success: false,
            error: "Não foi possível bloquear o usuário"
        });
    }
}

async function handleUnblockChannelUser(req, res) {
    if (!firebaseReady || !db || !auth) {
        sendHttpJson(res, 503, {
            success: false,
            error: "Serviço temporariamente indisponível"
        });
        return;
    }

    let decoded;
    try {
        decoded = await verifyBearerToken(req);
    } catch (_) {
        sendHttpJson(res, 401, {
            success: false,
            error: "Sessão inválida ou expirada"
        });
        return;
    }

    let body;
    try {
        body = await readJsonBody(req);
    } catch (error) {
        sendHttpJson(res, 400, {
            success: false,
            error: error.message
        });
        return;
    }

    const channelId = String(body.channelId || "").trim();
    const targetUid = String(body.userId || "").trim();

    if (!targetUid) {
        sendHttpJson(res, 400, {
            success: false,
            error: "Usuário inválido"
        });
        return;
    }

    try {
        const channel = await getManagedChannelOrRespond(
            res,
            decoded.uid,
            channelId
        );
        if (!channel) return;

        await db
            .ref(`channels/${channelId}/blockedUsers/${targetUid}`)
            .set(null);

        await db
            .ref(`channels/${channelId}/updatedAt`)
            .set(Date.now());

        await refreshConnectedClientChannels(targetUid);

        console.log(
            `[CHANNEL UNBLOCK USER] admin=${decoded.uid} ` +
            `target=${targetUid} channel=${channelId}`
        );

        sendHttpJson(res, 200, {
            success: true,
            channelId,
            userId: targetUid
        });

    } catch (error) {
        console.error("[CHANNEL UNBLOCK USER]", error.message);
        sendHttpJson(res, 500, {
            success: false,
            error: "Não foi possível desbloquear o usuário"
        });
    }
}

async function handleMakeChannelModerator(req, res) {
    if (!firebaseReady || !db || !auth) {
        sendHttpJson(res, 503, {
            success: false,
            error: "Serviço temporariamente indisponível"
        });
        return;
    }

    let decoded;
    try {
        decoded = await verifyBearerToken(req);
    } catch (_) {
        sendHttpJson(res, 401, {
            success: false,
            error: "Sessão inválida ou expirada"
        });
        return;
    }

    let body;
    try {
        body = await readJsonBody(req);
    } catch (error) {
        sendHttpJson(res, 400, {
            success: false,
            error: error.message
        });
        return;
    }

    const channelId = String(body.channelId || "").trim();
    const targetUid = String(body.userId || "").trim();

    if (!targetUid) {
        sendHttpJson(res, 400, {
            success: false,
            error: "Usuário inválido"
        });
        return;
    }

    try {
        const channel = await getOwnedChannelOrRespond(
            res,
            decoded.uid,
            channelId
        );
        if (!channel) return;

        if (targetUid === decoded.uid) {
            sendHttpJson(res, 400, {
                success: false,
                error: "Você já é o Administrador deste canal"
            });
            return;
        }

        if (
            channel.blockedUsers &&
            Object.prototype.hasOwnProperty.call(
                channel.blockedUsers,
                targetUid
            )
        ) {
            sendHttpJson(res, 409, {
                success: false,
                error: "Desbloqueie este usuário antes de torná-lo Moderador"
            });
            return;
        }

        const membership = await db
            .ref(`users/${targetUid}/channels/${channelId}`)
            .get();

        if (!membership.exists()) {
            sendHttpJson(res, 404, {
                success: false,
                error: "Este usuário não participa do canal"
            });
            return;
        }

        const now = Date.now();

        await db.ref().update({
            [`channels/${channelId}/moderators/${targetUid}`]: {
                addedAt: now,
                addedBy: decoded.uid,
                permissions: {
                    blockUsers: true,
                    unblockUsers: true
                }
            },

            /*
             * Limpa eventual registro da nomenclatura antiga.
             */
            [`channels/${channelId}/admins/${targetUid}`]: null,

            [`channels/${channelId}/updatedAt`]: now
        });

        const refreshedChannelSnapshot = await db
            .ref(`channels/${channelId}`)
            .get();

        if (refreshedChannelSnapshot.exists()) {
            updateInMemoryChannelAdmins(
                channelId,
                refreshedChannelSnapshot.val() || {}
            );
        }

        await refreshConnectedClientChannels(targetUid);
        broadcastUserLists();

        console.log(
            `[CHANNEL MAKE MODERATOR] owner=${decoded.uid} ` +
            `target=${targetUid} channel=${channelId}`
        );

        sendHttpJson(res, 200, {
            success: true,
            channelId,
            userId: targetUid
        });

    } catch (error) {
        console.error("[CHANNEL MAKE MODERATOR]", error.message);
        sendHttpJson(res, 500, {
            success: false,
            error: "Não foi possível tornar o usuário Moderador"
        });
    }
}

async function handleRemoveChannelModerator(req, res) {
    if (!firebaseReady || !db || !auth) {
        sendHttpJson(res, 503, {
            success: false,
            error: "Serviço temporariamente indisponível"
        });
        return;
    }

    let decoded;
    try {
        decoded = await verifyBearerToken(req);
    } catch (_) {
        sendHttpJson(res, 401, {
            success: false,
            error: "Sessão inválida ou expirada"
        });
        return;
    }

    let body;
    try {
        body = await readJsonBody(req);
    } catch (error) {
        sendHttpJson(res, 400, {
            success: false,
            error: error.message
        });
        return;
    }

    const channelId = String(body.channelId || "").trim();
    const targetUid = String(body.userId || "").trim();

    if (!targetUid) {
        sendHttpJson(res, 400, {
            success: false,
            error: "Usuário inválido"
        });
        return;
    }

    try {
        // Somente o Administrador/criador pode remover a função
        // de Moderação de outro usuário.
        const channel = await getOwnedChannelOrRespond(
            res,
            decoded.uid,
            channelId
        );
        if (!channel) return;

        if (targetUid === decoded.uid || isChannelOwnerUser(channel, targetUid)) {
            sendHttpJson(res, 400, {
                success: false,
                error: "O Administrador do canal não pode ter sua função removida"
            });
            return;
        }

        if (!isChannelModerator(channel, targetUid)) {
            sendHttpJson(res, 409, {
                success: false,
                error: "Este usuário não é Moderador deste canal"
            });
            return;
        }

        const now = Date.now();

        await db.ref().update({
            [`channels/${channelId}/moderators/${targetUid}`]: null,

            // Remove também eventual registro legado.
            [`channels/${channelId}/admins/${targetUid}`]: null,

            [`channels/${channelId}/updatedAt`]: now
        });

        const refreshedChannelSnapshot = await db
            .ref(`channels/${channelId}`)
            .get();

        if (refreshedChannelSnapshot.exists()) {
            updateInMemoryChannelAdmins(
                channelId,
                refreshedChannelSnapshot.val() || {}
            );
        }

        // Atualiza imediatamente a função exibida para quem está conectado,
        // sem remover o usuário do canal e sem alterar o estado ligado/desligado.
        await refreshConnectedClientChannels(targetUid);
        broadcastUserLists();

        console.log(
            `[CHANNEL REMOVE MODERATOR] owner=${decoded.uid} ` +
            `target=${targetUid} channel=${channelId}`
        );

        sendHttpJson(res, 200, {
            success: true,
            channelId,
            userId: targetUid
        });

    } catch (error) {
        console.error("[CHANNEL REMOVE MODERATOR]", error.message);
        sendHttpJson(res, 500, {
            success: false,
            error: "Não foi possível remover a Moderação"
        });
    }
}

async function handleListBlockedChannelUsers(req, res) {
    if (!firebaseReady || !db || !auth) {
        sendHttpJson(res, 503, {
            success: false,
            error: "Serviço temporariamente indisponível"
        });
        return;
    }

    let decoded;
    try {
        decoded = await verifyBearerToken(req);
    } catch (_) {
        sendHttpJson(res, 401, {
            success: false,
            error: "Sessão inválida ou expirada"
        });
        return;
    }

    const url = new URL(
        req.url,
        `http://${req.headers.host || "localhost"}`
    );
    const channelId = String(
        url.searchParams.get("channelId") || ""
    ).trim();

    try {
        const channel = await getManagedChannelOrRespond(
            res,
            decoded.uid,
            channelId
        );
        if (!channel) return;

        const blockedUsers = channel.blockedUsers || {};
        const result = [];

        for (const [uid, infoValue] of Object.entries(blockedUsers)) {
            const info = infoValue || {};
            let profile = {};

            try {
                profile = await getUserProfile(uid);
            } catch (_) {
                profile = {};
            }

            const liveClient = clients.get(uid);
            const name = String(
                profile.username ||
                profile.name ||
                liveClient?.name ||
                "Usuário"
            ).trim() || "Usuário";

            const avatar = sanitizeAvatar(
                profile.avatar ||
                profile.avatarData ||
                profile.photoUrl ||
                liveClient?.avatar ||
                ""
            );

            result.push({
                id: uid,
                name,
                avatar,
                blockedAt: Number(info.blockedAt || 0)
            });
        }

        result.sort((a, b) =>
            (b.blockedAt || 0) - (a.blockedAt || 0)
        );

        sendHttpJson(res, 200, {
            success: true,
            channelId,
            users: result
        });

    } catch (error) {
        console.error("[CHANNEL BLOCKED USERS]", error.message);
        sendHttpJson(res, 500, {
            success: false,
            error: "Não foi possível carregar os usuários bloqueados"
        });
    }
}

async function handleSetChannelEnabled(req, res) {
    if (!firebaseReady || !db || !auth) {
        sendHttpJson(res, 503, {
            success: false,
            error: "Serviço temporariamente indisponível"
        });
        return;
    }

    let decoded;
    try {
        decoded = await verifyBearerToken(req);
    } catch (_) {
        sendHttpJson(res, 401, {
            success: false,
            error: "Sessão inválida ou expirada"
        });
        return;
    }

    let body;
    try {
        body = await readJsonBody(req);
    } catch (error) {
        sendHttpJson(res, 400, {
            success: false,
            error: error.message
        });
        return;
    }

    const channelId = String(body.channelId || "").trim();
    const enabled = body.enabled === true;

    if (!isValidChannelId(channelId)) {
        sendHttpJson(res, 400, {
            success: false,
            error: "ID de canal inválido"
        });
        return;
    }

    try {
        const channelSnapshot = await db
            .ref(`channels/${channelId}`)
            .get();

        if (!channelSnapshot.exists()) {
            sendHttpJson(res, 404, {
                success: false,
                error: "Canal não encontrado"
            });
            return;
        }

        const channel = channelSnapshot.val() || {};

        if (
            enabled &&
            channel.blockedUsers &&
            Object.prototype.hasOwnProperty.call(
                channel.blockedUsers,
                decoded.uid
            )
        ) {
            sendHttpJson(res, 403, {
                success: false,
                code: "CHANNEL_BLOCKED",
                error: "Você não pode entrar neste canal, pois foi bloqueado nele."
            });
            return;
        }

        const membershipRef = db
            .ref(`users/${decoded.uid}/channels/${channelId}`);

        const snapshot = await membershipRef.get();

        if (!snapshot.exists()) {
            sendHttpJson(res, 404, {
                success: false,
                error: "Canal não está na sua lista"
            });
            return;
        }

        const membership = snapshot.val() || {};

        await membershipRef.update({
            enabled,
            addedAt: Number(membership.addedAt || Date.now())
        });

        // Um canal desligado não pode continuar sendo o canal padrão.
        if (!enabled) {
            const defaultRef = db
                .ref(`users/${decoded.uid}/defaultChannelId`);

            const defaultSnapshot = await defaultRef.get();

            if (
                defaultSnapshot.exists() &&
                String(defaultSnapshot.val() || "") === channelId
            ) {
                await defaultRef.set(null);
            }
        }

        await refreshConnectedClientChannels(decoded.uid);

        sendHttpJson(res, 200, {
            success: true,
            channelId,
            enabled
        });
    } catch (error) {
        console.error("[CHANNEL ENABLE]", error.message);
        sendHttpJson(res, 500, {
            success: false,
            error: "Não foi possível alterar o estado do canal"
        });
    }
}

async function handleSetDefaultChannel(req, res) {
    if (!firebaseReady || !db || !auth) {
        sendHttpJson(res, 503, {
            success: false,
            error: "Serviço temporariamente indisponível"
        });
        return;
    }

    let decoded;
    try {
        decoded = await verifyBearerToken(req);
    } catch (_) {
        sendHttpJson(res, 401, {
            success: false,
            error: "Sessão inválida ou expirada"
        });
        return;
    }

    let body;
    try {
        body = await readJsonBody(req);
    } catch (error) {
        sendHttpJson(res, 400, {
            success: false,
            error: error.message
        });
        return;
    }

    const channelId = String(body.channelId || "").trim();

    if (!isValidChannelId(channelId)) {
        sendHttpJson(res, 400, {
            success: false,
            error: "ID de canal inválido"
        });
        return;
    }

    try {
        const channelSnapshot = await db
            .ref(`channels/${channelId}`)
            .get();

        if (!channelSnapshot.exists()) {
            sendHttpJson(res, 404, {
                success: false,
                error: "Canal não encontrado"
            });
            return;
        }

        const channel = channelSnapshot.val() || {};

        if (
            channel.blockedUsers &&
            Object.prototype.hasOwnProperty.call(
                channel.blockedUsers,
                decoded.uid
            )
        ) {
            sendHttpJson(res, 403, {
                success: false,
                code: "CHANNEL_BLOCKED",
                error: "Você não pode entrar neste canal, pois foi bloqueado nele."
            });
            return;
        }

        const membership = await db
            .ref(`users/${decoded.uid}/channels/${channelId}`)
            .get();

        if (!membership.exists()) {
            sendHttpJson(res, 404, {
                success: false,
                error: "Canal não está na sua lista"
            });
            return;
        }

        const membershipData = membership.val() || {};

        if (membershipData.enabled === false) {
            sendHttpJson(res, 409, {
                success: false,
                error: "Ative o canal antes de defini-lo como padrão"
            });
            return;
        }

        await db
            .ref(`users/${decoded.uid}/defaultChannelId`)
            .set(channelId);

        await refreshConnectedClientChannels(decoded.uid);

        sendHttpJson(res, 200, {
            success: true,
            defaultChannelId: channelId
        });
    } catch (error) {
        console.error("[CHANNEL DEFAULT]", error.message);
        sendHttpJson(res, 500, {
            success: false,
            error: "Não foi possível definir o canal padrão"
        });
    }
}


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
                        256 * 1024
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

            // ------------------------------------------------
            // USER PROFILE / AVATAR
            // ------------------------------------------------

            if (
                req.method ===
                    "GET" &&
                req.url.split("?")[0] ===
                    "/api/users/me"
            ) {

                await handleGetUserMe(
                    req,
                    res
                );

                return;
            }

            if (
                req.method ===
                    "POST" &&
                req.url ===
                    "/api/users/avatar"
            ) {

                await handleSetUserAvatar(
                    req,
                    res
                );

                return;
            }

            // ------------------------------------------------
            // CHANNEL LIST
            // ------------------------------------------------

            if (
                req.method ===
                    "GET" &&
                req.url.split("?")[0] ===
                    "/api/channels/mine"
            ) {

                await handleListUserChannels(
                    req,
                    res
                );

                return;
            }

            // ------------------------------------------------
            // CHANNEL SEARCH
            // ------------------------------------------------

            if (
                req.method ===
                    "GET" &&
                req.url.split("?")[0] ===
                    "/api/channels/search"
            ) {

                await handleSearchPublicChannels(
                    req,
                    res
                );

                return;
            }

            // ------------------------------------------------
            // CHANNEL CREATE
            // ------------------------------------------------

            if (
                req.method ===
                    "POST" &&
                req.url ===
                    "/api/channels/create"
            ) {

                await handleCreateChannel(
                    req,
                    res
                );

                return;
            }

            // ------------------------------------------------
            // CHANNEL ADD
            // ------------------------------------------------

            if (
                req.method ===
                    "POST" &&
                req.url ===
                    "/api/channels/add"
            ) {

                await handleAddChannel(
                    req,
                    res
                );

                return;
            }

            // ------------------------------------------------
            // CHANNEL AVATAR
            // ------------------------------------------------

            if (
                req.method ===
                    "POST" &&
                req.url ===
                    "/api/channels/avatar"
            ) {

                await handleSetChannelAvatar(
                    req,
                    res
                );

                return;
            }

            // ------------------------------------------------
            // CHANNEL DELETE
            // ------------------------------------------------

            if (
                req.method ===
                    "POST" &&
                req.url ===
                    "/api/channels/delete"
            ) {

                await handleDeleteChannel(
                    req,
                    res
                );

                return;
            }

            // ------------------------------------------------
            // CHANNEL REMOVE FROM USER LIST
            // ------------------------------------------------

            if (
                req.method ===
                    "POST" &&
                req.url ===
                    "/api/channels/remove"
            ) {

                await handleRemoveChannel(
                    req,
                    res
                );

                return;
            }

            // ------------------------------------------------
            // CHANNEL ADMIN - CONNECTED / BLOCKED USERS
            // ------------------------------------------------

            if (
                req.method === "POST" &&
                req.url === "/api/channels/disconnect-user"
            ) {
                await handleDisconnectChannelUser(req, res);
                return;
            }

            if (
                req.method === "POST" &&
                req.url === "/api/channels/block-user"
            ) {
                await handleBlockChannelUser(req, res);
                return;
            }

            if (
                req.method === "POST" &&
                (
                    req.url === "/api/channels/make-moderator" ||
                    req.url === "/api/channels/make-admin"
                )
            ) {
                await handleMakeChannelModerator(req, res);
                return;
            }

            if (
                req.method === "POST" &&
                (
                    req.url === "/api/channels/remove-moderator" ||
                    req.url === "/api/channels/remove-admin"
                )
            ) {
                await handleRemoveChannelModerator(req, res);
                return;
            }

            if (
                req.method === "POST" &&
                req.url === "/api/channels/unblock-user"
            ) {
                await handleUnblockChannelUser(req, res);
                return;
            }

            if (
                req.method === "GET" &&
                req.url.split("?")[0] === "/api/channels/blocked-users"
            ) {
                await handleListBlockedChannelUsers(req, res);
                return;
            }

            // ------------------------------------------------
            // CHANNEL ENABLE/DISABLE
            // ------------------------------------------------

            if (
                req.method ===
                    "POST" &&
                req.url ===
                    "/api/channels/set-enabled"
            ) {

                await handleSetChannelEnabled(
                    req,
                    res
                );

                return;
            }

            // ------------------------------------------------
            // CHANNEL DEFAULT
            // ------------------------------------------------

            if (
                req.method ===
                    "POST" &&
                req.url ===
                    "/api/channels/set-default"
            ) {

                await handleSetDefaultChannel(
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

function isClientOnChannel(client, channelId) {
    return (
        isOpen(client) &&
        !!channelId &&
        client.activeChannelId === channelId &&
        client.enabledChannelIds?.has(channelId)
    );
}

function clientListFor(ws) {
    const channelId = ws?.activeChannelId || null;

    if (!channelId) {
        return [];
    }

    const channel = Array.isArray(ws?.channels)
        ? ws.channels.find(item => String(item.id) === String(channelId))
        : null;

    const ownerUid =
        String(
            channel?.ownerUid || ""
        );

    const moderatorUids =
        new Set(
            Array.isArray(channel?.moderatorUids)
                ? channel.moderatorUids.map(
                    value => String(value)
                )
                : (
                    Array.isArray(channel?.adminUids)
                        ? channel.adminUids.map(
                            value => String(value)
                        )
                        : []
                )
        );

    return [
        ...clients.values()
    ]
        .filter(
            client =>
                isClientOnChannel(
                    client,
                    channelId
                )
        )
        .map(client => {
            const clientUid =
                String(
                    client.userId || ""
                );

            const isOwner =
                !!clientUid &&
                clientUid === ownerUid;

            const isModerator =
                !isOwner &&
                moderatorUids.has(
                    clientUid
                );

            return {
                id:
                    client.userId,

                name:
                    client.name,

                avatar:
                    client.avatar || "",

                isOwner,

                isModerator,

                /*
                 * Compatibilidade com Android antigo.
                 * Na versão nova, Moderador é lido por isModerator.
                 */
                isAdmin:
                    isOwner ||
                    isModerator,

                role:
                    isOwner
                        ? "owner"
                        : (
                            isModerator
                                ? "moderator"
                                : "user"
                        )
            };
        });
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
    const payload = JSON.stringify(data);

    for (const ws of clients.values()) {
        if (
            isOpen(ws) &&
            ws.userId !== exceptId
        ) {
            try {
                ws.send(payload);
            } catch (error) {
                console.error(
                    `[BROADCAST ERROR] ${error.message}`
                );
            }
        }
    }
}

function broadcastJsonToChannel(
    channelId,
    data,
    exceptId = null
) {
    const payload = JSON.stringify(data);

    for (const ws of clients.values()) {
        if (
            !isClientOnChannel(ws, channelId) ||
            ws.userId === exceptId
        ) {
            continue;
        }

        try {
            ws.send(payload);
        } catch (error) {
            console.error(
                `[CHANNEL BROADCAST ERROR] channel=${channelId} ` +
                `user=${ws.userId} ${error.message}`
            );
        }
    }
}

function broadcastUserLists() {
    for (const ws of clients.values()) {
        if (!isOpen(ws)) {
            continue;
        }

        sendJson(ws, {
            type: "user_list",
            channelId: ws.activeChannelId || null,
            clients: clientListFor(ws)
        });
    }
}

function activeTransmittersForClient(ws) {
    const result = [];

    for (const [channelId, state] of activeTransmitters) {
        if (
            !isClientOnChannel(ws, channelId) ||
            state.userId === ws.userId
        ) {
            continue;
        }

        result.push({
            channelId,
            id: state.userId,
            name: state.name || state.userId,
            avatar: state.avatar || ""
        });
    }

    return result;
}

function buildChannelStateMessage(ws) {
    return {
        type: "channel_state",
        channels: Array.isArray(ws.channels) ? ws.channels : [],
        defaultChannelId: ws.defaultChannelId || null,
        activeChannelId: ws.activeChannelId || null,
        activeTransmitters: activeTransmittersForClient(ws)
    };
}

/*
 * ============================================================
 * SINCRONIZAÇÃO DE USUÁRIO QUE ENTRA NO MEIO DE UMA TRANSMISSÃO
 * ============================================================
 *
 * Quando o start_tx original aconteceu antes deste WebSocket existir,
 * o cliente novo recebia apenas activeTransmitters no init/channel_state.
 * A interface sabia que alguém estava falando, mas o Android não passava
 * pelo mesmo fluxo normal de start_tx que é usado quando a transmissão
 * começa depois que o cliente já está conectado.
 *
 * Agora o servidor envia, somente para o cliente que acabou de entrar
 * no canal, um start_tx de sincronização ANTES de continuar o fluxo live.
 */
function sendCurrentTransmitterStart(
    ws,
    channelId
) {
    if (
        !isOpen(ws) ||
        !ws.userId ||
        !channelId ||
        !isClientOnChannel(ws, channelId)
    ) {
        return false;
    }

    const active =
        activeTransmitters.get(channelId);

    if (
        !active ||
        active.userId === ws.userId
    ) {
        return false;
    }

    const channel =
        Array.isArray(ws.channels)
            ? ws.channels.find(
                item =>
                    String(item.id) ===
                    String(channelId)
            )
            : null;

    sendJson(
        ws,
        {
            type: "start_tx",
            from: active.userId,
            name: active.name || active.userId,
            avatar: active.avatar || "",
            channelId,
            channelName: channel?.name || "Canal",

            /*
             * O Android usa este marcador para saber que não é uma
             * nova transmissão; ele entrou no meio de uma já ativa.
             */
            lateJoin: true
        }
    );

    const bootstrapPackets =
        Array.isArray(active.bootstrapPackets)
            ? active.bootstrapPackets
            : [];

    let bootstrapDelivered =
        0;

    /*
     * Envia imediatamente, logo depois do start_tx lateJoin.
     * Como isto acontece de forma síncrona no mesmo turno do event loop,
     * estes pacotes entram na fila do WebSocket antes dos próximos frames
     * live processados pelo servidor.
     */
    for (const packet of bootstrapPackets) {
        try {
            ws.send(
                packet,
                {
                    binary: true
                }
            );

            bootstrapDelivered++;

        } catch (error) {
            console.error(
                `[LATE JOIN BOOTSTRAP ERROR] ` +
                `listener=${ws.userId} ` +
                `channel=${channelId} ` +
                `${error.message}`
            );

            break;
        }
    }

    console.log(
        `[LATE JOIN TX SYNC] listener=${ws.userId} ` +
        `speaker=${active.userId} channel=${channelId} ` +
        `bootstrap=${bootstrapDelivered}`
    );

    return true;
}

// ============================================================
// AUDIO VALIDATION
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
// RESET TRANSMITTER
// ============================================================

function resetTransmitterIf(
    userId,
    exceptId = null
) {
    const stopped = [];

    for (const [channelId, state] of activeTransmitters) {
        if (state.userId !== userId) {
            continue;
        }

        activeTransmitters.delete(channelId);
        stopped.push({ channelId, state });

        console.log(
            `[TX RESET] user=${userId} channel=${channelId}`
        );

        broadcastJsonToChannel(
            channelId,
            {
                type: "stop_tx",
                from: userId,
                channelId
            },
            exceptId
        );
    }

    const ws = clients.get(userId);
    if (ws) {
        ws.txChannelId = null;
    }

    return stopped;
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

            const sameLogicalSession =
                old.sessionId === sessionId &&
                old.deviceId === deviceId;

            console.log(
                sameLogicalSession
                    ? `[IDENTIFY] renovando conexão da mesma sessão ${userId}`
                    : `[IDENTIFY] substituindo conexão anterior de ${userId}`
            );

            try {

                if (sameLogicalSession) {
                    /*
                     * Uma queda/reconexão do mesmo aparelho não é uma
                     * revogação de sessão. Fechamos o socket antigo sem
                     * mandar session_revoked para não bloquear o cliente.
                     */
                    old.close(
                        1000,
                        "Connection refreshed"
                    );

                } else {
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
                }

            } catch (_) {
            }

            /*
             * Se o socket antigo estava transmitindo, encerra somente esse
             * estado antes de registrar a nova conexão.
             */
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

        // Avatar do usuário é apenas metadado visual.
        // Se o perfil ainda não tiver avatar, usamos o picture do token (quando existir)
        // e, na ausência dos dois, o Android mostra a inicial do nome.
        ws.avatar = sanitizeAvatar(decoded.picture || "");

        try {
            const profile = await getUserProfile(userId);
            ws.avatar = sanitizeAvatar(
                profile.avatar ||
                profile.avatarData ||
                profile.photoUrl ||
                ws.avatar
            );
        } catch (_) {
            // Mantém o avatar do token ou vazio.
        }

        try {
            const channelState =
                await loadClientChannelState(userId);

            ws.channels =
                channelState.channels;

            ws.enabledChannelIds =
                channelState.enabledChannelIds;

            ws.blockedChannelIds =
                channelState.blockedChannelIds;

            ws.defaultChannelId =
                channelState.defaultChannelId;

            // Ao conectar pela primeira vez, o canal padrão é a seleção
            // inicial. A Activity pode trocar imediatamente via select_channel.
            ws.activeChannelId =
                ws.defaultChannelId &&
                ws.enabledChannelIds.has(ws.defaultChannelId)
                    ? ws.defaultChannelId
                    : null;

        } catch (error) {
            console.error(
                `[IDENTIFY CHANNELS] uid=${userId} ${error.message}`
            );

            ws.channels = [];
            ws.enabledChannelIds = new Set();
            ws.blockedChannelIds = new Set();
            ws.defaultChannelId = null;
            ws.activeChannelId = null;
        }

        ws.txChannelId =
            null;

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

        const activeForClient =
            activeTransmittersForClient(ws);

        sendJson(
            ws,
            {
                type:
                    "init",

                id:
                    userId,

                channelId:
                    ws.activeChannelId || null,

                clients:
                    clientListFor(ws),

                channels:
                    ws.channels,

                defaultChannelId:
                    ws.defaultChannelId || null,

                activeChannelId:
                    ws.activeChannelId || null,

                activeTransmitters:
                    activeForClient,

                // Compatibilidade com versões antigas do Android.
                activeTransmitter:
                    activeForClient.length > 0
                        ? {
                            id: activeForClient[0].id,
                            name: activeForClient[0].name,
                            avatar: activeForClient[0].avatar || "",
                            channelId: activeForClient[0].channelId
                        }
                        : null
            }
        );

        /*
         * IMPORTANTE: esta chamada acontece imediatamente depois do init,
         * no mesmo fluxo do servidor. Assim o cliente recebe o start_tx
         * de sincronização antes dos próximos pacotes live do transmissor.
         */
        if (ws.activeChannelId) {
            sendCurrentTransmitterStart(
                ws,
                ws.activeChannelId
            );
        }

        broadcastUserLists();

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

        console.log(
            `[NAME IGNORED] ${ws.userId} tentou alterar o nome público`
        );

        return;
    }

    // ========================================================
    // SELECT ACTIVE CHANNEL
    // ========================================================

    if (
        data.type ===
        "select_channel"
    ) {
        const requestedChannelId =
            String(data.channelId || "").trim();

        if (
            requestedChannelId &&
            ws.blockedChannelIds?.has(requestedChannelId)
        ) {
            sendJson(
                ws,
                {
                    type: "channel_select_denied",
                    code: "CHANNEL_BLOCKED",
                    channelId: requestedChannelId,
                    message: "Você não pode entrar neste canal, pois foi bloqueado nele."
                }
            );
            return;
        }

        if (
            !requestedChannelId ||
            !ws.enabledChannelIds?.has(requestedChannelId)
        ) {
            sendJson(
                ws,
                {
                    type: "channel_select_denied",
                    channelId: requestedChannelId || null,
                    message: "Este canal não está ativo."
                }
            );
            return;
        }

        if (
            ws.txChannelId &&
            ws.txChannelId !== requestedChannelId
        ) {
            resetTransmitterIf(ws.userId);
        }

        ws.activeChannelId =
            requestedChannelId;

        sendJson(
            ws,
            {
                type: "channel_selected",
                channelId: requestedChannelId
            }
        );

        sendJson(
            ws,
            buildChannelStateMessage(ws)
        );

        /*
         * Se o canal selecionado já está ocupado, reproduz o start_tx
         * somente para este cliente recém-chegado ao canal.
         */
        sendCurrentTransmitterStart(
            ws,
            requestedChannelId
        );

        broadcastUserLists();
        return;
    }

    // ========================================================
    // START TX
    // ========================================================

    if (
        data.type ===
        "start_tx"
    ) {
        const requestedChannelId =
            String(data.channelId || "").trim();

        const channelId =
            requestedChannelId ||
            ws.activeChannelId ||
            ws.defaultChannelId ||
            null;

        if (
            channelId &&
            ws.blockedChannelIds?.has(channelId)
        ) {
            sendJson(
                ws,
                {
                    type: "tx_denied",
                    code: "CHANNEL_BLOCKED",
                    channelId,
                    message: "Você não pode entrar neste canal, pois foi bloqueado nele."
                }
            );
            return;
        }

        if (
            !channelId ||
            !ws.enabledChannelIds?.has(channelId)
        ) {
            sendJson(
                ws,
                {
                    type: "tx_denied",
                    code: "NO_ACTIVE_CHANNEL",
                    message: "Selecione um canal ativo antes de transmitir."
                }
            );

            return;
        }

        // O pacote de controle e a tela aberta precisam apontar para
        // o mesmo canal. Isso impede TX em um canal enquanto a interface
        // está mostrando outro.
        if (
            ws.activeChannelId &&
            channelId !== ws.activeChannelId
        ) {
            sendJson(
                ws,
                {
                    type: "tx_denied",
                    code: "INVALID_CHANNEL",
                    message: "O canal de transmissão não corresponde ao canal aberto."
                }
            );

            return;
        }

        ws.activeChannelId =
            channelId;

        const active =
            activeTransmitters.get(channelId);

        if (
            active &&
            active.userId !== ws.userId
        ) {
            console.log(
                `[TX DENIED] ${ws.userId} tentou transmitir ` +
                `channel=${channelId}; ocupado por ${active.userId}`
            );

            sendJson(
                ws,
                {
                    type: "tx_denied",
                    code: "CHANNEL_BUSY",
                    channelId,
                    name: active.name || active.userId,
                    avatar: active.avatar || "",
                    message: "Canal ocupado"
                }
            );

            return;
        }

        // Um mesmo usuário transmite em apenas um canal por vez.
        if (
            ws.txChannelId &&
            ws.txChannelId !== channelId
        ) {
            resetTransmitterIf(ws.userId);
        }

        const state = {
            userId: ws.userId,
            name: ws.name,
            avatar: ws.avatar || "",
            startedAt: Date.now(),
            audioPacketCount: 0,
            audioBytesRelayed: 0,

            /*
             * Cópias dos primeiros frames binários deste TX.
             * São usadas somente para inicializar um usuário
             * que entrar no canal com a fala já em andamento.
             */
            bootstrapPackets: []
        };

        activeTransmitters.set(
            channelId,
            state
        );

        ws.txChannelId =
            channelId;

        const channel =
            (ws.channels || [])
                .find(item => String(item.id) === channelId);

        console.log(
            `[TX START] ${ws.userId} (${ws.name}) channel=${channelId}`
        );

        broadcastJsonToChannel(
            channelId,
            {
                type: "start_tx",
                from: ws.userId,
                name: ws.name,
                avatar: ws.avatar || "",
                channelId,
                channelName: channel?.name || "Canal"
            },
            ws.userId
        );

        return;
    }

    // ========================================================
    // STOP TX
    // ========================================================

    if (
        data.type ===
        "stop_tx"
    ) {
        const channelId =
            ws.txChannelId || ws.activeChannelId || null;

        const state = channelId
            ? activeTransmitters.get(channelId)
            : null;

        if (
            state &&
            state.userId === ws.userId
        ) {
            const duration =
                state.startedAt > 0
                    ? Date.now() - state.startedAt
                    : 0;

            console.log(
                `[TX STOP] ${ws.userId} (${ws.name}) | ` +
                `channel=${channelId} | ` +
                `pacotes=${state.audioPacketCount} | ` +
                `bytes=${state.audioBytesRelayed} | ` +
                `duração=${duration} ms`
            );
        }

        resetTransmitterIf(
            ws.userId,
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

        ws.avatar =
            "";

        ws.channels =
            [];

        ws.enabledChannelIds =
            new Set();

        ws.blockedChannelIds =
            new Set();

        ws.defaultChannelId =
            null;

        ws.activeChannelId =
            null;

        ws.txChannelId =
            null;

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
                    const channelId =
                        ws.txChannelId || null;

                    const txState =
                        channelId
                            ? activeTransmitters.get(channelId)
                            : null;

                    if (
                        !ws.userId ||
                        !ws.sessionId ||
                        !channelId ||
                        !txState ||
                        txState.userId !== ws.userId ||
                        !ws.enabledChannelIds?.has(channelId)
                    ) {
                        console.warn(
                            `[AUDIO DROP] pacote rejeitado ` +
                            `user=${ws.userId || "não identificado"} ` +
                            `channel=${channelId || "nenhum"}`
                        );

                        return;
                    }

                    txState.audioPacketCount++;
                    txState.audioBytesRelayed += data.length;

                    /*
                     * Preserva somente os primeiros frames do TX.
                     * Buffer.from cria uma cópia independente do buffer
                     * recebido pelo WebSocket.
                     */
                    if (
                        Array.isArray(txState.bootstrapPackets) &&
                        txState.bootstrapPackets.length <
                            LATE_JOIN_BOOTSTRAP_PACKETS
                    ) {
                        txState.bootstrapPackets.push(
                            Buffer.from(data)
                        );
                    }

                    let delivered = 0;

                    for (const client of clients.values()) {
                        if (
                            isClientOnChannel(client, channelId) &&
                            client.userId !== ws.userId
                        ) {
                            try {
                                client.send(
                                    data,
                                    {
                                        binary: true
                                    }
                                );

                                delivered++;

                            } catch (error) {
                                console.error(
                                    `[AUDIO TX ERROR] ` +
                                    `channel=${channelId} ` +
                                    `para=${client.userId} ` +
                                    `${error.message}`
                                );
                            }
                        }
                    }

                    if (
                        txState.audioPacketCount === 1 ||
                        txState.audioPacketCount % 100 === 0
                    ) {
                        const opusSize =
                            data.length - AUDIO_HEADER_SIZE;

                        console.log(
                            `[AUDIO] RX #${txState.audioPacketCount} ` +
                            `de=${ws.userId} ` +
                            `channel=${channelId} ` +
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

                const wasCurrentConnection =
                    clients.get(
                        ws.userId
                    ) === ws;

                if (
                    wasCurrentConnection
                ) {

                    clients.delete(
                        ws.userId
                    );

                    resetTransmitterIf(
                        ws.userId
                    );

                    broadcastUserLists();
                }
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
        let packetCount = 0;
        let byteCount = 0;

        for (const state of activeTransmitters.values()) {
            packetCount += Number(state.audioPacketCount || 0);
            byteCount += Number(state.audioBytesRelayed || 0);
        }

        if (packetCount > 0) {
            console.log(
                `[AUDIO STATS] ` +
                `canaisAtivos=${activeTransmitters.size} ` +
                `pacotes=${packetCount} ` +
                `bytes=${byteCount}`
            );
        }
    },
    60_000
);
