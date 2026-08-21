import type { KVNamespace } from '@cloudflare/workers-types';
import type { DeleteTask } from '../telegram/auto-delete';
import type { EmailCache, EmailHandleStatus } from '../types';
import {
    DELETE_CURSOR_KEY,
    DELETE_KEY_PREFIX,
    DELETE_KEY_TTL_BUFFER,
    deleteKey,
    nextPurgeKey,
    parseDeleteKey,
    resolvePurgeSlot,
} from '../telegram/auto-delete';

export type AddressListStoreKey = 'BLOCK_LIST' | 'WHITE_LIST';

export class Dao {
    private readonly db: KVNamespace;

    constructor(db: KVNamespace) {
        this.db = db;
        this.loadArrayFromDB = this.loadArrayFromDB.bind(this);
        this.addAddress = this.addAddress.bind(this);
        this.removeAddress = this.removeAddress.bind(this);
        this.loadMailStatus = this.loadMailStatus.bind(this);
        this.loadMailCache = this.loadMailCache.bind(this);
    }

    async loadArrayFromDB(key: AddressListStoreKey): Promise<string[]> {
        try {
            const raw = await this.db.get(key);
            return loadArrayFromRaw(raw);
        } catch (e) {
            console.error(e);
        }
        return [];
    }

    async addAddress(address: string, type: AddressListStoreKey): Promise<void> {
        const list = await this.loadArrayFromDB(type);
        list.unshift(address);
        await this.db.put(type, JSON.stringify(list));
    }

    async removeAddress(address: string, type: AddressListStoreKey): Promise<void> {
        const list = await this.loadArrayFromDB(type);
        const result = list.filter(item => item !== address);
        await this.db.put(type, JSON.stringify(result));
    }

    async loadMailStatus(id: string, guardian: boolean): Promise<EmailHandleStatus> {
        const defaultStatus = {
            telegram: false,
            forward: [],
        };
        if (guardian) {
            try {
                const raw = await this.db.get(id);
                if (raw) {
                    return {
                        ...defaultStatus,
                        ...JSON.parse(raw),
                    };
                }
            } catch (e) {
                console.error(e);
            }
        }
        return defaultStatus;
    }

    async saveMailStatus(id: string, status: EmailHandleStatus, ttl?: number): Promise<void> {
        await this.db.put(id, JSON.stringify(status), { expirationTtl: ttl });
    }

    async loadMailCache(id: string): Promise<EmailCache | null> {
        try {
            const raw = await this.db.get(id);
            if (raw) {
                return JSON.parse(raw);
            }
        } catch (e) {
            console.error(e);
        }
        return null;
    }

    async saveMailCache(id: string, cache: EmailCache, ttl?: number): Promise<void> {
        await this.db.put(id, JSON.stringify(cache), { expirationTtl: ttl });
    }

    async telegramIDToMailID(id: string): Promise<string | null> {
        return await this.db.get(`TelegramID2MailID:${id}`);
    }

    async saveTelegramIDToMailID(id: string, mailID: string, ttl?: number): Promise<void> {
        await this.db.put(`TelegramID2MailID:${id}`, mailID, { expirationTtl: ttl });
    }

    async enqueueGroupMessageDelete(chatId: number, messageId: number, ttlSeconds: number, sentAtSec: number): Promise<void> {
        const nextKey = nextPurgeKey(chatId);
        const existingRaw = await this.db.get(nextKey);
        const existingNext = existingRaw ? Number.parseInt(existingRaw, 10) : Number.NaN;
        const slot = resolvePurgeSlot(
            sentAtSec,
            ttlSeconds,
            Number.isFinite(existingNext) ? existingNext : null,
        );
        if (`${slot}` !== existingRaw) {
            const nextTtl = Math.max(60, slot - sentAtSec + DELETE_KEY_TTL_BUFFER);
            await this.db.put(nextKey, `${slot}`, { expirationTtl: nextTtl });
        }
        const key = deleteKey(slot, chatId, messageId);
        const keyTtl = Math.max(60, slot - sentAtSec + DELETE_KEY_TTL_BUFFER);
        await this.db.put(key, '1', {
            expirationTtl: keyTtl,
            metadata: { sent: sentAtSec },
        });
    }

    async listDeleteSlot(slot: number, maxKeys: number = 200): Promise<{ tasks: DeleteTask[]; complete: boolean }> {
        const prefix = `${DELETE_KEY_PREFIX}${slot}:`;
        const tasks: DeleteTask[] = [];
        let cursor: string | undefined;
        do {
            const page = await this.db.list({ prefix, cursor, limit: 100 });
            for (const item of page.keys) {
                const parsed = parseDeleteKey(item.name);
                if (!parsed) {
                    continue;
                }
                const metadata = item.metadata as { sent?: number } | undefined;
                tasks.push({
                    key: item.name,
                    slot: parsed.slot,
                    chatId: parsed.chatId,
                    messageId: parsed.messageId,
                    sentAt: typeof metadata?.sent === 'number' ? metadata.sent : parsed.slot,
                });
                if (tasks.length >= maxKeys) {
                    return { tasks, complete: false };
                }
            }
            cursor = page.list_complete ? undefined : page.cursor;
        } while (cursor);
        return { tasks, complete: true };
    }

    async deleteKeys(keys: string[]): Promise<void> {
        for (const key of keys) {
            await this.db.delete(key);
        }
    }

    async loadDeleteCursor(): Promise<number | null> {
        const raw = await this.db.get(DELETE_CURSOR_KEY);
        if (!raw) {
            return null;
        }
        const value = Number.parseInt(raw, 10);
        return Number.isFinite(value) ? value : null;
    }

    async saveDeleteCursor(slot: number): Promise<void> {
        await this.db.put(DELETE_CURSOR_KEY, `${slot}`);
    }

    async clearDeleteCursor(): Promise<void> {
        await this.db.delete(DELETE_CURSOR_KEY);
    }
}

export function loadArrayFromRaw(raw: string | null): string[] {
    if (!raw) {
        return [];
    }
    let list = [];
    try {
        list = JSON.parse(raw);
    } catch {
        return [];
    }
    if (!Array.isArray(list)) {
        return [];
    }
    return list;
}
