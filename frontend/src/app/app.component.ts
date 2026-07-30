import { Component, OnDestroy, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Subscription } from 'rxjs';
import {
  BridgeMessage,
  ConnectionStatus,
  KafkaBridgeService,
} from './kafka-bridge.service';
import {
  RabbitMessage,
  RabbitStompService,
  StompConnectionStatus,
} from './rabbit-stomp.service';

type BrokerTab = 'kafka' | 'rabbit';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css',
})
export class AppComponent implements OnInit, OnDestroy {
  private readonly kafka = inject(KafkaBridgeService);
  private readonly rabbit = inject(RabbitStompService);
  private readonly subs = new Subscription();

  title = 'Message brokers ↔ Angular';
  tab: BrokerTab = 'rabbit';

  kafkaStatus: ConnectionStatus = 'disconnected';
  kafkaDraft = '';
  kafkaMessages: BridgeMessage[] = [];

  rabbitStatus: StompConnectionStatus = 'disconnected';
  rabbitDraft = '';
  rabbitMessages: RabbitMessage[] = [];

  readonly rabbitQueue = RabbitStompService.QUEUE;
  readonly rabbitWs = RabbitStompService.WS_URL;

  ngOnInit(): void {
    this.subs.add(
      this.kafka.status$.subscribe((s) => (this.kafkaStatus = s))
    );
    this.subs.add(
      this.kafka.messages$.subscribe((msg) => {
        this.kafkaMessages = [msg, ...this.kafkaMessages].slice(0, 100);
      })
    );
    this.subs.add(
      this.rabbit.status$.subscribe((s) => (this.rabbitStatus = s))
    );
    this.subs.add(
      this.rabbit.messages$.subscribe((msg) => {
        this.rabbitMessages = [msg, ...this.rabbitMessages].slice(0, 100);
      })
    );
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
    this.kafka.disconnect();
    this.rabbit.disconnect();
  }

  selectTab(tab: BrokerTab): void {
    this.tab = tab;
  }

  connectKafka(): void {
    this.kafka.connect();
  }

  disconnectKafka(): void {
    this.kafka.disconnect();
  }

  sendKafka(): void {
    const text = this.kafkaDraft.trim();
    if (!text) {
      return;
    }
    try {
      this.kafka.send(text);
      this.kafkaDraft = '';
    } catch (err) {
      console.error(err);
    }
  }

  connectRabbit(): void {
    this.rabbit.connect();
  }

  disconnectRabbit(): void {
    this.rabbit.disconnect();
  }

  sendRabbit(): void {
    const text = this.rabbitDraft.trim();
    if (!text) {
      return;
    }
    try {
      this.rabbit.send(text);
      this.rabbitDraft = '';
    } catch (err) {
      console.error(err);
    }
  }

  statusLabel(status: ConnectionStatus | StompConnectionStatus): string {
    switch (status) {
      case 'connected':
        return 'Connecté';
      case 'connecting':
        return 'Connexion…';
      case 'error':
        return 'Erreur';
      default:
        return 'Déconnecté';
    }
  }

  formatJson(value: unknown): string {
    return JSON.stringify(value, null, 2);
  }
}
