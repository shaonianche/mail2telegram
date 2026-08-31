export const DELETE_SLOT_SECONDS = 300;
export const GROUP_MESSAGE_TTL_MIN = 300;
export const GROUP_MESSAGE_TTL_MAX = 165600; // 46 hours, stays inside Telegram's 48h delete window
export const DELETE_KEY_TTL_BUFFER = 7200;
export const TELEGRAM_DELETE_MAX_AGE = 48 * 3600;
export const MAX_SLOTS_PER_TICK = 6;
export const DELETE_BATCH_SIZE = 25;
export const MAX_TELEGRAM_FETCHES_PER_TICK = 40;
export const DELETE_CURSOR_KEY = 'del:cursor';
export const DELETE_KEY_PREFIX = 'del:';
export const DELETE_NEXT_KEY_PREFIX = 'del:next:';
export const DELETE_ACTIVE_KEY = 'del:active';

export function nextPurgeKey(chatId: number): string {
    return `${DELETE_NEXT_KEY_PREFIX}${chatId}`;
}

export function resolvePurgeSlot(sentAtSec: number, ttlSeconds: number, existingNextSlot: number | null): number {
    const fresh = slotForExpireAt(sentAtSec + ttlSeconds);
    if (existingNextSlot !== null && existingNextSlot > sentAtSec) {
        return existingNextSlot;
    }
    return fresh;
}

/**
 * Sentinel slot marker (`del:active`) so idle cron ticks can skip KV list() calls.
 * Returns the slot to persist, or null when the existing marker already covers it.
 */
export function resolveDeleteActiveSlot(existingRaw: string | null, slot: number): number | null {
    const existing = existingRaw === null ? Number.NaN : Number.parseInt(existingRaw, 10);
    const current = Number.isFinite(existing) ? existing : 0;
    if (slot > current) {
        return slot;
    }
    return null;
}

export interface DeleteTask {
    key: string;
    slot: number;
    chatId: number;
    messageId: number;
    sentAt: number;
}

export type TelegramDeleteAction = 'ok' | 'drop' | 'drop-chat' | 'retry';

export function parseGroupMessageTtl(raw: string | undefined): number {
    if (raw === undefined || raw === '') {
        return 0;
    }
    const value = Number.parseInt(raw, 10);
    if (!Number.isFinite(value) || value <= 0) {
        if (raw.trim() !== '0') {
            console.warn(`[auto-delete] invalid GROUP_MESSAGE_TTL=${raw}, disable enqueue`);
        }
        return 0;
    }
    return Math.min(GROUP_MESSAGE_TTL_MAX, Math.max(GROUP_MESSAGE_TTL_MIN, value));
}

export function isGroupChatType(type: string | undefined): boolean {
    return type === 'group' || type === 'supergroup';
}

export function alignSlot(unixSeconds: number): number {
    return Math.floor(unixSeconds / DELETE_SLOT_SECONDS) * DELETE_SLOT_SECONDS;
}

export function previousSlot(scheduledTimeMs: number): number {
    return alignSlot(Math.floor(scheduledTimeMs / 1000)) - DELETE_SLOT_SECONDS;
}

export function slotForExpireAt(expireAtSec: number): number {
    return alignSlot(expireAtSec);
}

export function deleteKey(slot: number, chatId: number, messageId: number): string {
    return `${DELETE_KEY_PREFIX}${slot}:${chatId}:${messageId}`;
}

export function parseDeleteKey(name: string): { slot: number; chatId: number; messageId: number } | null {
    const match = /^del:(\d+):(-?\d+):(\d+)$/.exec(name);
    if (!match) {
        return null;
    }
    return {
        slot: Number(match[1]),
        chatId: Number(match[2]),
        messageId: Number(match[3]),
    };
}

export function shouldSkipTelegramDelete(sentAt: number, nowSec: number): boolean {
    return nowSec - sentAt >= TELEGRAM_DELETE_MAX_AGE;
}

export function classifyTelegramDeleteError(status: number, errorCode?: number): TelegramDeleteAction {
    const code = errorCode || status;
    if (code === 429 || status === 429) {
        return 'retry';
    }
    if (code === 403 || status === 403) {
        return 'drop-chat';
    }
    if (code === 400 || status === 400) {
        return 'drop';
    }
    if (status >= 500 || code >= 500) {
        return 'retry';
    }
    return 'retry';
}

export function groupTasksByChat(tasks: DeleteTask[]): Map<number, DeleteTask[]> {
    const groups = new Map<number, DeleteTask[]>();
    for (const task of tasks) {
        const list = groups.get(task.chatId) || [];
        list.push(task);
        groups.set(task.chatId, list);
    }
    return groups;
}

export function batches<T>(items: T[], size: number): T[][] {
    const result: T[][] = [];
    for (let i = 0; i < items.length; i += size) {
        result.push(items.slice(i, i + size));
    }
    return result;
}

export function selectSlotsToDrain(scheduledTimeMs: number, storedCursor: number | null): number[] {
    const dueEnd = previousSlot(scheduledTimeMs);
    const oldest = alignSlot(dueEnd - TELEGRAM_DELETE_MAX_AGE);
    if (storedCursor === null || storedCursor > dueEnd) {
        const overlap = [dueEnd - DELETE_SLOT_SECONDS, dueEnd].filter(slot => slot >= oldest);
        return overlap;
    }
    const start = Math.max(storedCursor, oldest);
    const slots: number[] = [];
    for (let slot = start; slot <= dueEnd && slots.length < MAX_SLOTS_PER_TICK; slot += DELETE_SLOT_SECONDS) {
        slots.push(slot);
    }
    return slots;
}

export type CursorPersist = 'put' | 'delete' | 'none';

export function nextCursorPersist(params: {
    storedCursor: number | null;
    dueEnd: number;
    incompleteSlot: number | null;
    lastCompletedSlot: number | null;
}): { cursor: number | null; persist: CursorPersist } {
    const { storedCursor, dueEnd, incompleteSlot, lastCompletedSlot } = params;
    if (incompleteSlot !== null) {
        if (storedCursor === incompleteSlot) {
            return { cursor: incompleteSlot, persist: 'none' };
        }
        return { cursor: incompleteSlot, persist: 'put' };
    }
    if (lastCompletedSlot !== null && lastCompletedSlot < dueEnd) {
        const cursor = lastCompletedSlot + DELETE_SLOT_SECONDS;
        if (storedCursor === cursor) {
            return { cursor, persist: 'none' };
        }
        return { cursor, persist: 'put' };
    }
    if (storedCursor !== null) {
        return { cursor: null, persist: 'delete' };
    }
    return { cursor: null, persist: 'none' };
}

export interface DrainDeps {
    listSlot: (slot: number) => Promise<{ tasks: DeleteTask[]; complete: boolean }>;
    deleteKeys: (keys: string[]) => Promise<void>;
    deleteMessages: (chatId: number, messageIds: number[]) => Promise<TelegramDeleteAction>;
    nowSec: number;
    maxFetches?: number;
    batchSize?: number;
}

export async function drainDueSlots(slots: number[], deps: DrainDeps): Promise<{
    incompleteSlot: number | null;
    lastCompletedSlot: number | null;
    fetches: number;
}> {
    const maxFetches = deps.maxFetches ?? MAX_TELEGRAM_FETCHES_PER_TICK;
    const batchSize = deps.batchSize ?? DELETE_BATCH_SIZE;
    let fetches = 0;
    let lastCompletedSlot: number | null = null;

    for (const slot of slots) {
        const listed = await deps.listSlot(slot);
        const remaining = listed.tasks.filter((task) => {
            if (shouldSkipTelegramDelete(task.sentAt, deps.nowSec)) {
                return false;
            }
            return true;
        });
        const expiredKeys = listed.tasks
            .filter(task => shouldSkipTelegramDelete(task.sentAt, deps.nowSec))
            .map(task => task.key);
        if (expiredKeys.length > 0) {
            await deps.deleteKeys(expiredKeys);
        }

        const byChat = groupTasksByChat(remaining);
        let slotIncomplete = !listed.complete;
        let stopSlot = false;

        for (const [chatId, chatTasks] of byChat) {
            if (stopSlot) {
                break;
            }
            let skipChat = false;
            for (const batch of batches(chatTasks, batchSize)) {
                if (skipChat) {
                    break;
                }
                if (fetches >= maxFetches) {
                    slotIncomplete = true;
                    stopSlot = true;
                    break;
                }
                fetches += 1;
                const action = await deps.deleteMessages(chatId, batch.map(task => task.messageId));
                if (action === 'ok' || action === 'drop') {
                    await deps.deleteKeys(batch.map(task => task.key));
                    continue;
                }
                if (action === 'drop-chat') {
                    const rest = remaining.filter(task => task.chatId === chatId).map(task => task.key);
                    await deps.deleteKeys(rest);
                    skipChat = true;
                    break;
                }
                slotIncomplete = true;
                stopSlot = true;
                break;
            }
        }

        if (slotIncomplete) {
            return { incompleteSlot: slot, lastCompletedSlot, fetches };
        }
        lastCompletedSlot = slot;
    }

    return { incompleteSlot: null, lastCompletedSlot, fetches };
}
