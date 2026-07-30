import { Injectable, OnDestroy } from '@angular/core';
import { BehaviorSubject, Observable, Subject } from 'rxjs';

export type ConnectionStatus = 'disconnected' | 'connecting' | 'connected' | 'error';

export interface BridgeMessage {
  source?: string;
  type?: string;
  topic?: string;
  partition?: number;
  offset?: number;
  key?: string | null;
  value?: unknown;
  message?: string;
  text?: string;
  [key: string]: unknown;
}

@Injectable({ providedIn: 'root' })
export class KafkaBridgeService implements OnDestroy {
  private readonly wsUrl =
    (typeof window !== 'undefined' &&
      (window as Window & { __KAFKA_WS_URL__?: string }).__KAFKA_WS_URL__) ||
    'ws://localhost:8000/ws';

  private socket: WebSocket | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private shouldReconnect = false;
  private readonly messagesSubject = new Subject<BridgeMessage>();
  private readonly statusSubject = new BehaviorSubject<ConnectionStatus>('disconnected');

  readonly messages$: Observable<BridgeMessage> = this.messagesSubject.asObservable();
  readonly status$: Observable<ConnectionStatus> = this.statusSubject.asObservable();

  connect(): void {
    if (
      this.socket &&
      (this.socket.readyState === WebSocket.OPEN ||
        this.socket.readyState === WebSocket.CONNECTING)
    ) {
      return;
    }

    this.shouldReconnect = true;
    this.statusSubject.next('connecting');
    this.socket = new WebSocket(this.wsUrl);

    this.socket.onopen = () => {
      this.statusSubject.next('connected');
    };

    this.socket.onmessage = (event: MessageEvent<string>) => {
      try {
        const parsed = JSON.parse(event.data) as BridgeMessage;
        this.messagesSubject.next(parsed);
      } catch {
        this.messagesSubject.next({ source: 'raw', text: event.data });
      }
    };

    this.socket.onerror = () => {
      this.statusSubject.next('error');
    };

    this.socket.onclose = () => {
      this.statusSubject.next('disconnected');
      this.socket = null;
      if (this.shouldReconnect) {
        this.reconnectTimer = setTimeout(() => this.connect(), 2000);
      }
    };
  }

  disconnect(): void {
    this.shouldReconnect = false;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.socket?.close();
    this.socket = null;
    this.statusSubject.next('disconnected');
  }

  send(text: string, key = 'angular'): void {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      throw new Error('WebSocket non connecté');
    }
    this.socket.send(JSON.stringify({ text, key }));
  }

  ngOnDestroy(): void {
    this.disconnect();
    this.messagesSubject.complete();
    this.statusSubject.complete();
  }
}
