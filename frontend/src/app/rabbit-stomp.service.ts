import { Injectable, OnDestroy } from '@angular/core';
import { BehaviorSubject, Observable, Subject, Subscription } from 'rxjs';
import { IMessage, RxStomp, RxStompState } from '@stomp/rx-stomp';

export type StompConnectionStatus =
  | 'disconnected'
  | 'connecting'
  | 'connected'
  | 'error';

export interface RabbitMessage {
  source: 'rabbitmq' | 'stomp';
  destination?: string;
  body: string;
  headers?: Record<string, string>;
  receivedAt: string;
}

@Injectable({ providedIn: 'root' })
export class RabbitStompService implements OnDestroy {
  static readonly QUEUE = '/queue/demo-angular';
  static readonly WS_URL = 'ws://localhost:15674/ws';

  private readonly rxStomp = new RxStomp();
  private readonly messagesSubject = new Subject<RabbitMessage>();
  private readonly statusSubject = new BehaviorSubject<StompConnectionStatus>(
    'disconnected'
  );
  private watchSub: Subscription | null = null;
  private stateSub: Subscription | null = null;

  readonly messages$: Observable<RabbitMessage> =
    this.messagesSubject.asObservable();
  readonly status$: Observable<StompConnectionStatus> =
    this.statusSubject.asObservable();

  connect(): void {
    if (this.rxStomp.active) {
      return;
    }

    this.statusSubject.next('connecting');

    this.rxStomp.configure({
      brokerURL: RabbitStompService.WS_URL,
      connectHeaders: {
        login: 'guest',
        passcode: 'guest',
      },
      heartbeatIncoming: 10000,
      heartbeatOutgoing: 10000,
      reconnectDelay: 3000,
      debug: () => undefined,
    });

    this.stateSub?.unsubscribe();
    this.stateSub = this.rxStomp.connectionState$.subscribe((state) => {
      switch (state) {
        case RxStompState.OPEN:
          this.statusSubject.next('connected');
          break;
        case RxStompState.CONNECTING:
          this.statusSubject.next('connecting');
          break;
        case RxStompState.CLOSED:
          this.statusSubject.next('disconnected');
          break;
        default:
          break;
      }
    });

    this.rxStomp.activate();

    this.watchSub?.unsubscribe();
    this.watchSub = this.rxStomp
      .watch({ destination: RabbitStompService.QUEUE })
      .subscribe((message: IMessage) => {
        this.messagesSubject.next({
          source: 'rabbitmq',
          destination: RabbitStompService.QUEUE,
          body: message.body,
          headers: message.headers as Record<string, string>,
          receivedAt: new Date().toISOString(),
        });
      });
  }

  disconnect(): void {
    this.watchSub?.unsubscribe();
    this.watchSub = null;
    this.stateSub?.unsubscribe();
    this.stateSub = null;
    void this.rxStomp.deactivate();
    this.statusSubject.next('disconnected');
  }

  send(text: string): void {
    if (!this.rxStomp.connected()) {
      throw new Error('STOMP non connecté');
    }
    const payload = JSON.stringify({
      type: 'ui.message',
      text,
      ts: new Date().toISOString(),
    });
    this.rxStomp.publish({
      destination: RabbitStompService.QUEUE,
      body: payload,
      headers: { 'content-type': 'application/json' },
    });
  }

  ngOnDestroy(): void {
    this.disconnect();
    this.messagesSubject.complete();
    this.statusSubject.complete();
  }
}
