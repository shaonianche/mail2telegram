import { fetchHandler } from './handler/fetch';
import { emailHandler } from './handler/mail';
import { scheduledHandler } from './handler/scheduled';
import './polyfill';

export default {
    fetch: fetchHandler,
    email: emailHandler,
    scheduled: scheduledHandler,
};
