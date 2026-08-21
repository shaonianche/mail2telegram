import type { ForwardableEmailMessage } from '@cloudflare/workers-types';
import type * as Telegram from 'telegram-bot-api-types';
import type { BlockPolicy, EmailCache, Environment } from '../../types';
import { Dao } from '../../db';
import { isMessageBlock, parseEmail, renderEmailListMode } from '../../mail';
import { createTelegramBotAPI, isGroupChatType, parseGroupMessageTtl } from '../../telegram';

interface TelegramApiResponse<T> {
    ok: boolean;
    result?: T;
    description?: string;
}

async function enqueueGroupMessageDelete(
    dao: Dao,
    env: Environment,
    message: Telegram.Message,
): Promise<void> {
    const ttl = parseGroupMessageTtl(env.GROUP_MESSAGE_TTL);
    if (ttl <= 0) {
        return;
    }
    if (!isGroupChatType(message.chat?.type)) {
        return;
    }
    const sentAt = message.date || Math.floor(Date.now() / 1000);
    await dao.enqueueGroupMessageDelete(message.chat.id, message.message_id, ttl, sentAt);
    console.log(`[mail] auto-delete.enqueue ${JSON.stringify({
        chatId: message.chat.id,
        messageId: message.message_id,
        ttl,
        sentAt,
    })}`);
}

export async function sendMailToTelegram(mail: EmailCache, env: Environment): Promise<number[]> {
    const {
        TELEGRAM_TOKEN,
        TELEGRAM_ID,
        DB,
    } = env;
    const req = await renderEmailListMode(mail, env);
    const api = createTelegramBotAPI(TELEGRAM_TOKEN);
    const dao = new Dao(DB);
    const messageID: number[] = [];
    let lastSendError: string | null = null;
    for (const rawId of TELEGRAM_ID.split(',')) {
        const chatId = rawId.trim();
        if (!chatId) {
            continue;
        }
        const msg = await api.sendMessageWithReturns({
            chat_id: chatId,
            ...req,
        }) as TelegramApiResponse<Telegram.Message>;
        if (!msg?.ok || msg.result?.message_id == null) {
            lastSendError = msg?.description || 'sendMessage failed';
            console.error(`[mail] send_message.failed ${JSON.stringify({
                chatId,
                ok: msg?.ok,
                description: lastSendError,
            })}`);
            continue;
        }
        messageID.push(msg.result.message_id);
        try {
            await enqueueGroupMessageDelete(dao, env, msg.result);
        } catch (error) {
            console.error(`[mail] auto-delete.enqueue_failed ${JSON.stringify({
                chatId: msg.result.chat?.id,
                messageId: msg.result.message_id,
                message: (error as Error).message,
            })}`);
        }
    }
    if (messageID.length === 0) {
        throw new Error(lastSendError || 'Failed to send mail to telegram');
    }
    return messageID;
}

export async function emailHandler(message: ForwardableEmailMessage, env: Environment): Promise<void> {
    const {
        FORWARD_LIST,
        BLOCK_POLICY,
        GUARDIAN_MODE,
        DB,
        MAIL_TTL,
        MAX_EMAIL_SIZE,
        MAX_EMAIL_SIZE_POLICY,
    } = env;

    const dao = new Dao(DB);
    const id = message.headers.get('Message-ID')?.trim() || crypto.randomUUID();
    const isBlock = await isMessageBlock(message, env);
    const isGuardian = GUARDIAN_MODE === 'true';
    const blockPolicy: BlockPolicy[] = (BLOCK_POLICY || 'telegram').split(',') as BlockPolicy[];
    const statusTTL = 60 * 60;
    const status = await dao.loadMailStatus(id, isGuardian);

    // Reject the email
    if (isBlock && blockPolicy.includes('reject')) {
        message.setReject('Blocked');
        return;
    }

    // Forward to email
    try {
        const blockForward = isBlock && blockPolicy.includes('forward');
        const forwardList = blockForward ? [] : (FORWARD_LIST || '').split(',');
        for (const forward of forwardList) {
            try {
                const add = forward.trim();
                if (status.forward.includes(add)) {
                    continue;
                }
                await message.forward(add);
                if (isGuardian) {
                    status.forward.push(add);
                    await dao.saveMailStatus(id, status, statusTTL);
                }
            } catch (e) {
                console.error(e);
            }
        }
    } catch (e) {
        console.error(e);
    }

    // Send to Telegram
    try {
        const blockTelegram = isBlock && blockPolicy.includes('telegram');
        if (!status.telegram && !blockTelegram) {
            const ttl = Number.parseInt(MAIL_TTL, 10) || 60 * 60 * 24;
            const maxSize = Number.parseInt(MAX_EMAIL_SIZE || '', 10) || 512 * 1024;
            const maxSizePolicy = MAX_EMAIL_SIZE_POLICY || 'truncate';
            const mail = await parseEmail(message, maxSize, maxSizePolicy);
            await dao.saveMailCache(mail.id, mail, ttl);
            const msgIDs = await sendMailToTelegram(mail, env);
            for (const msgID of msgIDs) {
                await dao.saveTelegramIDToMailID(`${msgID}`, mail.id, ttl);
            }
        }
        if (isGuardian) {
            status.telegram = true;
            await dao.saveMailStatus(id, status, statusTTL);
        }
    } catch (e) {
        console.error(e);
    }
}
