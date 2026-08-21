import type { DeleteTask } from './auto-delete';
import {
    classifyTelegramDeleteError,
    DELETE_SLOT_SECONDS,
    deleteKey,
    drainDueSlots,
    GROUP_MESSAGE_TTL_MAX,
    GROUP_MESSAGE_TTL_MIN,
    isGroupChatType,
    nextCursorPersist,
    parseDeleteKey,
    parseGroupMessageTtl,
    previousSlot,
    selectSlotsToDrain,
    shouldSkipTelegramDelete,
} from './auto-delete';

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) {
        throw new Error(message);
    }
}

function assertEqual<T>(actual: T, expected: T, message: string): void {
    if (actual !== expected) {
        throw new Error(`${message}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
    }
}

export async function testAutoDelete(): Promise<void> {
    assertEqual(parseGroupMessageTtl(undefined), 0, 'unset ttl');
    assertEqual(parseGroupMessageTtl(''), 0, 'empty ttl');
    assertEqual(parseGroupMessageTtl('0'), 0, 'zero ttl');
    assertEqual(parseGroupMessageTtl('abc'), 0, 'invalid ttl');
    assertEqual(parseGroupMessageTtl('100'), GROUP_MESSAGE_TTL_MIN, 'clamp min');
    assertEqual(parseGroupMessageTtl('999999'), GROUP_MESSAGE_TTL_MAX, 'clamp max');
    assertEqual(parseGroupMessageTtl('3600'), 3600, 'hour ttl');

    assert(isGroupChatType('group'), 'group');
    assert(isGroupChatType('supergroup'), 'supergroup');
    assert(!isGroupChatType('private'), 'private');
    assert(!isGroupChatType('channel'), 'channel');
    assert(!isGroupChatType(undefined), 'missing type');

    const nowSec = 1_700_000_400;
    assertEqual(nowSec % DELETE_SLOT_SECONDS, 0, 'aligned fixture');
    const scheduledTimeMs = nowSec * 1000;
    const dueEnd = previousSlot(scheduledTimeMs);
    assertEqual(dueEnd, nowSec - DELETE_SLOT_SECONDS, 'previous slot');

    const overlap = selectSlotsToDrain(scheduledTimeMs, null);
    assertEqual(overlap.length, 2, 'idle overlap size');
    assertEqual(overlap[0], dueEnd - DELETE_SLOT_SECONDS, 'overlap start');
    assertEqual(overlap[1], dueEnd, 'overlap end');

    const behindStart = dueEnd - DELETE_SLOT_SECONDS * 10;
    const behind = selectSlotsToDrain(scheduledTimeMs, behindStart);
    assertEqual(behind.length, 6, 'max slots per tick');
    assertEqual(behind[0], behindStart, 'behind starts at cursor');
    assertEqual(behind[5], behindStart + DELETE_SLOT_SECONDS * 5, 'behind last slot');
    assert(behind[behind.length - 1] < dueEnd, 'behind does not skip to dueEnd');

    const key = deleteKey(dueEnd, -100123, 42);
    assertEqual(key, `del:${dueEnd}:-100123:42`, 'delete key format');
    const parsed = parseDeleteKey(key);
    assert(parsed, 'parse delete key');
    assertEqual(parsed.chatId, -100123, 'negative chat id');
    assertEqual(parsed.messageId, 42, 'message id');
    assertEqual(parseDeleteKey('del:cursor'), null, 'cursor is not a task key');

    assert(shouldSkipTelegramDelete(nowSec - 48 * 3600, nowSec), '48h skip');
    assert(!shouldSkipTelegramDelete(nowSec - 46 * 3600, nowSec), '46h keep');

    assertEqual(classifyTelegramDeleteError(400, 400), 'drop', '400 drop');
    assertEqual(classifyTelegramDeleteError(403, 403), 'drop-chat', '403 drop chat');
    assertEqual(classifyTelegramDeleteError(429, 429), 'retry', '429 retry');
    assertEqual(classifyTelegramDeleteError(500, 500), 'retry', '500 retry');

    assertEqual(
        nextCursorPersist({ storedCursor: null, dueEnd, incompleteSlot: dueEnd, lastCompletedSlot: null }).persist,
        'put',
        'incomplete puts cursor',
    );
    assertEqual(
        nextCursorPersist({ storedCursor: dueEnd, dueEnd, incompleteSlot: dueEnd, lastCompletedSlot: null }).persist,
        'none',
        'same incomplete cursor skips put',
    );
    assertEqual(
        nextCursorPersist({
            storedCursor: behindStart,
            dueEnd,
            incompleteSlot: null,
            lastCompletedSlot: behindStart + DELETE_SLOT_SECONDS * 5,
        }).persist,
        'put',
        'still behind puts next slot',
    );
    assertEqual(
        nextCursorPersist({ storedCursor: behindStart, dueEnd, incompleteSlot: null, lastCompletedSlot: dueEnd }).persist,
        'delete',
        'caught up clears cursor',
    );
    assertEqual(
        nextCursorPersist({ storedCursor: null, dueEnd, incompleteSlot: null, lastCompletedSlot: dueEnd }).persist,
        'none',
        'idle caught up does not write',
    );

    const tasks: DeleteTask[] = [
        { key: deleteKey(dueEnd, -1001, 1), slot: dueEnd, chatId: -1001, messageId: 1, sentAt: nowSec - 3600 },
        { key: deleteKey(dueEnd, -1001, 2), slot: dueEnd, chatId: -1001, messageId: 2, sentAt: nowSec - 3600 },
        { key: deleteKey(dueEnd, -1001, 3), slot: dueEnd, chatId: -1001, messageId: 3, sentAt: nowSec - 48 * 3600 },
    ];
    const deletedKeys: string[] = [];
    const deletedIds: number[][] = [];
    const drain = await drainDueSlots([dueEnd], {
        nowSec,
        batchSize: 25,
        maxFetches: 40,
        listSlot: async () => ({ tasks, complete: true }),
        deleteKeys: async (keys) => {
            deletedKeys.push(...keys);
        },
        deleteMessages: async (_chatId, messageIds) => {
            deletedIds.push(messageIds);
            return 'ok';
        },
    });
    assertEqual(drain.incompleteSlot, null, 'drain complete');
    assertEqual(drain.lastCompletedSlot, dueEnd, 'drain last slot');
    assertEqual(JSON.stringify(deletedIds), JSON.stringify([[1, 2]]), 'expired skipped from api');
    assert(deletedKeys.includes(tasks[2].key), 'expired kv dropped');
    assert(deletedKeys.includes(tasks[0].key), 'deleted kv dropped');

    const retryDrain = await drainDueSlots([dueEnd], {
        nowSec,
        listSlot: async () => ({ tasks: tasks.slice(0, 1), complete: true }),
        deleteKeys: async () => {},
        deleteMessages: async () => 'retry',
    });
    assertEqual(retryDrain.incompleteSlot, dueEnd, '429 keeps slot');
}

testAutoDelete().then(() => {
    console.log('auto-delete tests ok');
}).catch((error) => {
    console.error(error);
    process.exit(1);
});
