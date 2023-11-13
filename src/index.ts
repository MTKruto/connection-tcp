import { Socket } from "net";
import { Mutex } from "async-mutex";
import {
  ConnectionUnframed,
  getDcId,
  getDcIps,
  TransportAbridged,
  TransportProvider,
} from "@mtkruto/node";

const errConnectionNotOpen = new Error("Connection not open");

export class ConnectionTCP extends ConnectionUnframed
  implements ConnectionUnframed {
  #hostname: string;
  #port: number;
  #socket?: Socket;
  #rMutex = new Mutex();
  #wMutex = new Mutex();
  #buffer = new Array<number>();
  #nextResolve: [
    number,
    { resolve: () => void; reject: (err: unknown) => void },
  ] | null = null;

  constructor(hostname: string, port: number) {
    super();
    this.#hostname = hostname;
    this.#port = port;
  }

  #rejectRead() {
    if (this.#nextResolve != null) {
      this.#nextResolve[1].reject(errConnectionNotOpen);
      this.#nextResolve = null;
    }
  }

  open() {
    this.#socket = new Socket();
    this.#socket.on("close", () => {
      this.#rejectRead();
      this.stateChangeHandler?.(false);
    });
    const mutex = new Mutex();
    this.#socket.on("data", async (data) => {
      const release = await mutex.acquire();

      for (const byte of data) {
        this.#buffer.push(byte);
      }

      if (
        this.#nextResolve != null && this.#buffer.length >= this.#nextResolve[0]
      ) {
        const resolve = this.#nextResolve[1].resolve;
        this.#nextResolve = null;
        resolve();
      }

      release();
    });
    return new Promise<void>((resolve, reject) => {
      this.#socket!.connect(this.#port, this.#hostname);
      this.#socket!.once("error", reject);
      this.#socket!.once(
        "connect",
        () => {
          this.#socket!.off("error", reject);
          resolve();
          this.stateChangeHandler?.(true);
        },
      );
    });
  }

  get connected() {
    return this.#socket?.readyState == "open";
  }

  #assertConnected() {
    if (!this.connected) {
      throw errConnectionNotOpen;
    }
  }

  async read(p: Uint8Array) {
    this.#assertConnected();
    const release = await this.#rMutex.acquire();
    try {
      this.#assertConnected();
      if (this.#buffer.length < p.length) {
        await new Promise<void>((resolve, reject) =>
          this.#nextResolve = [p.length, { resolve, reject }]
        );
      }
      p.set(this.#buffer.splice(0, p.length));
    } finally {
      release();
    }
  }

  async write(p: Uint8Array) {
    this.#assertConnected();
    const release = await this.#wMutex.acquire();
    try {
      this.#assertConnected();
      await new Promise<void>((resolve, reject) => {
        this.#socket!.write(
          p,
          (err) => {
            (err === undefined || err == null) ? resolve() : reject(err);
          },
        );
      });
    } finally {
      release();
    }
  }

  close() {
    this.#assertConnected();
    this.#socket!.destroy();
    this.#socket = undefined;
  }
}

export function transportProviderTcp(
  params?: { ipv6?: boolean; obfuscated?: boolean },
): TransportProvider {
  return ({ dc, cdn }) => {
    const connection = new ConnectionTCP(
      getDcIps(dc, params?.ipv6 ? "ipv6" : "ipv4")[0],
      80,
    );
    const transport = new TransportAbridged(connection, params?.obfuscated);
    return { connection, transport, dcId: getDcId(dc, cdn) };
  };
}
