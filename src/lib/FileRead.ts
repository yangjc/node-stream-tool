/**
 * YJC <https://github.com/yangjc> @2018-01-08
 */

'use strict';

import { open, read as fsRead, close, stat } from 'fs';
import { promisify } from 'util';

interface ReadingContext {
    file: string;
    sepLength: number;
    startPosition: number;
    lineCount: number;
    position: number; // 该行的开始位置
    isEnd: boolean;
}
// 计算下一行开始位置：ctx.position + buf.length + ctx.sepLength

interface Options {
    readLength?: number;
    sep?: string;
    position?: number;
}

interface LineCallback {
    (buf: Buffer, context: ReadingContext): Promise<boolean | void> | boolean | void;
}

export async function read(file: string, lineCallback: LineCallback, options?: Options): Promise<number> {
    options = options || {};
    const emptyBuf: Buffer = new Buffer(0);
    const sep: Buffer = Buffer.from(options.sep || '\n');
    const sepLen: number = sep.length;
    const length: number = options.readLength || 1048576; // 一次读取1M数据
    const startPosition: number = options.position || 0;

    const f: number = await promisify(open)(`${file}`, 'r');
    const context: ReadingContext = {
        file: file,
        sepLength: sepLen,
        startPosition: startPosition,
        lineCount: 0,
        position: startPosition,
        isEnd: false
    };

    let notEmpty: boolean = true;
    let broke: boolean = false;
    let position: number = startPosition;
    let rest: Buffer = emptyBuf;
    let offset: number;

    do {
        const buf: Buffer = new Buffer(rest.length + length);
        rest.length > 0 && rest.copy(buf);

        const result = await promisify(fsRead)(f, buf, rest.length, length, position);

        position += result.bytesRead;

        if (result.bytesRead === 0) {
            if (position === startPosition) {
                notEmpty = false;
            }
            break;
        }

        const bufLen: number = rest.length + result.bytesRead;

        let i: number;
        offset = 0;

        do {
            i = buf.indexOf(sep, offset);
            if (i === -1) {
                const b: Buffer = new Buffer(bufLen - offset);
                buf.copy(b, 0, offset);
                rest = b;
                break;
            }

            context.lineCount++;

            let r: boolean | void;
            if (i === offset) {
                r = await lineCallback(emptyBuf, context);
            } else {
                const b = new Buffer(i - offset);
                buf.copy(b, 0, offset, i);
                r = await lineCallback(b, context);
            }
            if (r === false) {
                broke = true;
                break;
            }

            context.position += (i - offset + sepLen);

            offset = i + sepLen;
            rest = emptyBuf;

        } while (true);

        if (broke || result.bytesRead < length) {
            break;
        }

    } while (true);

    if (notEmpty && !broke) {
        context.lineCount++;
        context.isEnd = true;
        await lineCallback(rest, context);
    }

    await promisify(close)(f);

    return position - startPosition;
}

export async function readFromEnd(file: string, lineCallback: LineCallback, options?: Options): Promise<number> {
    options = options || {};
    const emptyBuf: Buffer = new Buffer(0);
    const sep: Buffer = Buffer.from(options.sep || '\n');
    const sepLen: number = sep.length;
    const length: number = options.readLength || 1048576; // 一次读取1M数据

    // 表示忽略该位置（包含）之后的内容
    const startPosition: number = typeof options.position === 'number'
        ? options.position
        : (await promisify(stat)(file)).size;

    const f: number = await promisify(open)(file, 'r');
    const context: ReadingContext = {
        file: file,
        sepLength: sepLen,
        startPosition: startPosition,
        lineCount: 0,
        position: startPosition + sepLen,
        isEnd: false
    };

    let notEmpty: boolean = true;
    let broke: boolean = false;
    let bytesRead: number;
    let position: number = startPosition;
    let rest: Buffer = emptyBuf;
    let _i: number;

    do {
        const buf: Buffer = new Buffer(rest.length + length);

        if (position < length) {
            bytesRead = position;
            position = 0;
        } else {
            bytesRead = length;
            position -= length;
        }

        await promisify(fsRead)(f, buf, 0, bytesRead, position);

        if (bytesRead === 0) {
            if (position === 0) {
                notEmpty = false;
            }
            break;
        }

        rest.length > 0 && rest.copy(buf, bytesRead);

        const bufLen: number = rest.length + bytesRead;

        let i: number;
        _i = bufLen;

        do {
            i = _i < sepLen ? -1 : buf.lastIndexOf(sep, _i - sepLen);
            if (i === -1) {
                const b: Buffer = new Buffer(_i);
                buf.copy(b, 0, 0, _i);
                rest = b;
                break;
            }

            context.lineCount++;
            context.position -= _i - i;

            let r: boolean | void;
            if (i === _i - sepLen) {
                r = await lineCallback(emptyBuf, context);
            } else {
                const b = new Buffer(_i - i - sepLen);
                buf.copy(b, 0, i + sepLen, _i);
                r = await lineCallback(b, context);
            }
            if (r === false) {
                broke = true;
                break;
            }

            _i = i;
            rest = emptyBuf;

        } while (true);

        if (broke || position === 0) {
            break;
        }

    } while (true);

    if (notEmpty && !broke) {
        context.lineCount++;
        context.isEnd = true;
        context.position = 0;
        await lineCallback(rest, context);
    }

    await promisify(close)(f);

    return startPosition - position;
}
