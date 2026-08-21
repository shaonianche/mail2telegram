import type { ScheduledController } from '@cloudflare/workers-types';
import type { TelegramDeleteAction } from '../../telegram/auto-delete';
import type { Environment } from '../../types';
import { Dao } from '../../db';
import { createTelegramBotAPI } from '../../telegram';
import {
    classifyTelegramDeleteError,
    drainDueSlots,
    nextCursorPersist,
    previousSlot,
    selectSlotsToDrain,
} from '../../telegram/auto-delete';

function logScheduled(event: string, data?: Record<string, unknown>): void {
    console.log(`[scheduled] ${event}${data ? ` ${JSON.stringify(data)}` : ''}`);
}

async function telegramDeleteAction(
    api: ReturnType<typeof createTelegramBotAPI>,
    chatId: number,
    messageIds: number[],
): Promise<TelegramDeleteAction> {
    try {
        const result = await api.requestJSON<
            { chat_id: number; message_ids: number[] },
            { ok?: boolean; error_code?: number; description?: string }
        >('deleteMessages', {
            chat_id: chatId,
            message_ids: messageIds,
        });
        if (result?.ok) {
            return 'ok';
        }
        const action = classifyTelegramDeleteError(0, result?.error_code);
        logScheduled('delete_messages.result', {
            chatId,
            count: messageIds.length,
            ok: result?.ok,
            errorCode: result?.error_code,
            description: result?.description,
            action,
        });
        return action;
    } catch (error) {
        logScheduled('delete_messages.error', {
            chatId,
            count: messageIds.length,
            message: (error as Error).message,
        });
        return 'retry';
    }
}

export async function scheduledHandler(controller: ScheduledController, env: Environment): Promise<void> {
    const dao = new Dao(env.DB);
    const api = createTelegramBotAPI(env.TELEGRAM_TOKEN);
    const nowSec = Math.floor(controller.scheduledTime / 1000);
    const dueEnd = previousSlot(controller.scheduledTime);
    const storedCursor = await dao.loadDeleteCursor();
    const slots = selectSlotsToDrain(controller.scheduledTime, storedCursor);

    logScheduled('drain.start', {
        cron: controller.cron,
        scheduledTime: controller.scheduledTime,
        storedCursor,
        dueEnd,
        slots,
    });

    const result = await drainDueSlots(slots, {
        nowSec,
        listSlot: slot => dao.listDeleteSlot(slot),
        deleteKeys: keys => dao.deleteKeys(keys),
        deleteMessages: (chatId, messageIds) => telegramDeleteAction(api, chatId, messageIds),
    });

    const persist = nextCursorPersist({
        storedCursor,
        dueEnd,
        incompleteSlot: result.incompleteSlot,
        lastCompletedSlot: result.lastCompletedSlot,
    });

    if (persist.persist === 'put' && persist.cursor !== null) {
        await dao.saveDeleteCursor(persist.cursor);
    } else if (persist.persist === 'delete') {
        await dao.clearDeleteCursor();
    }

    logScheduled('drain.done', {
        fetches: result.fetches,
        incompleteSlot: result.incompleteSlot,
        lastCompletedSlot: result.lastCompletedSlot,
        persist: persist.persist,
        cursor: persist.cursor,
    });
}
