import { Component, OnInit, inject } from '@angular/core';
import { DatePipe } from '@angular/common';
import { FormsModule } from '@angular/forms';

import {
  AlertLevel,
  DigestFrequency,
  Topic,
  TopicsApiService,
} from '../services/topics-api.service';

interface TopicForm {
  topic_name: string;
  keywordsText: string;
  sources: string[];
  alert_level: AlertLevel;
}

const SOURCE_OPTIONS = ['NSE', 'SEBI'];

const SUB_SOURCE_OPTIONS = [
  { value: 'SEBI', label: 'SEBI' },
  { value: 'SEBI_MASTER', label: 'Sebi Master' },
  { value: 'NSE', label: 'NSE' },
];

@Component({
  selector: 'app-alerts',
  imports: [FormsModule, DatePipe],
  templateUrl: './alerts.component.html',
  styleUrl: './alerts.component.css',
})
export class AlertsComponent implements OnInit {
  private topicsApi = inject(TopicsApiService);

  topics: Topic[] = [];
  isLoading = true;
  saving = false;
  listError = '';
  formError = '';

  editingId: string | null = null;
  form: TopicForm = this.emptyForm();

  digestFrequency: DigestFrequency = 'NONE';
  prefsError = '';

  sourceSubs: string[] = [];
  subsError = '';

  readonly sourceOptions = SOURCE_OPTIONS;
  readonly subSourceOptions = SUB_SOURCE_OPTIONS;

  ngOnInit(): void {
    this.loadTopics();
    this.loadPrefs();
    this.loadSubs();
  }

  loadTopics(): void {
    this.isLoading = true;
    this.topicsApi.getTopics().subscribe({
      next: (topics) => {
        this.topics = topics;
        this.isLoading = false;
      },
      error: () => {
        this.isLoading = false;
        this.listError = 'Failed to load topics. Refresh the page to try again.';
      },
    });
  }

  loadPrefs(): void {
    this.topicsApi.getNotificationPrefs().subscribe({
      next: (prefs) => {
        this.digestFrequency = prefs.digest_frequency;
      },
      error: () => {
        this.prefsError = 'Failed to load notification preferences.';
      },
    });
  }

  selectDigest(freq: DigestFrequency): void {
    if (this.digestFrequency === freq) return;
    const previous = this.digestFrequency;
    this.digestFrequency = freq;
    this.prefsError = '';
    this.topicsApi.updateNotificationPrefs(freq).subscribe({
      error: (err) => {
        this.digestFrequency = previous;
        this.prefsError = this.errorText(err, 'Could not save notification preferences');
      },
    });
  }

  toggleSource(source: string): void {
    const i = this.form.sources.indexOf(source);
    if (i >= 0) this.form.sources.splice(i, 1);
    else this.form.sources.push(source);
  }

  loadSubs(): void {
    this.topicsApi.getSourceSubscriptions().subscribe({
      next: (res) => {
        this.sourceSubs = res.sources;
      },
      error: () => {
        this.subsError = 'Failed to load source subscriptions.';
      },
    });
  }

  toggleSourceSub(value: string): void {
    const previous = [...this.sourceSubs];
    const i = this.sourceSubs.indexOf(value);
    if (i >= 0) this.sourceSubs.splice(i, 1);
    else this.sourceSubs.push(value);
    this.subsError = '';
    this.topicsApi.updateSourceSubscriptions([...this.sourceSubs]).subscribe({
      error: (err) => {
        this.sourceSubs = previous;
        this.subsError = this.errorText(err, 'Could not save source subscriptions');
      },
    });
  }

  onSubmit(): void {
    const topicName = this.form.topic_name.trim();
    if (!topicName) {
      this.formError = 'Name is required';
      return;
    }
    if (this.form.sources.length === 0) {
      this.formError = 'Select at least one source';
      return;
    }

    const payload = {
      topic_name: topicName,
      keywords: this.parseKeywords(this.form.keywordsText),
      sources: [...this.form.sources],
      alert_level: this.form.alert_level,
    };

    this.saving = true;
    this.formError = '';

    const request = this.editingId
      ? this.topicsApi.updateTopic(this.editingId, payload)
      : this.topicsApi.createTopic(payload);

    request.subscribe({
      next: () => {
        this.saving = false;
        this.resetForm();
        this.loadTopics();
      },
      error: (err) => {
        this.saving = false;
        this.formError = this.errorText(err, 'Could not save the topic');
      },
    });
  }

  startEdit(topic: Topic): void {
    this.editingId = topic.id;
    this.formError = '';
    this.form = {
      topic_name: topic.topic_name,
      keywordsText: topic.keywords.join(', '),
      sources: [...topic.sources],
      alert_level: topic.alert_level,
    };
    const behavior: ScrollBehavior =
      window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth';
    document.getElementById('topic-form')?.scrollIntoView({ behavior, block: 'start' });
  }

  cancelEdit(): void {
    this.resetForm();
  }

  onAlertChange(topic: Topic, alertLevel: AlertLevel): void {
    if (topic.alert_level === alertLevel) return;
    const previous = topic.alert_level;
    topic.alert_level = alertLevel;
    this.topicsApi.updateTopic(topic.id, { alert_level: alertLevel }).subscribe({
      error: (err) => {
        topic.alert_level = previous;
        this.listError = this.errorText(err, 'Could not change the alert level');
      },
    });
  }

  deleteTopic(topic: Topic): void {
    if (!confirm(`Delete "${topic.topic_name}"? This can't be undone.`)) return;
    this.topicsApi.deleteTopic(topic.id).subscribe({
      next: () => {
        if (this.editingId === topic.id) this.resetForm();
        this.topics = this.topics.filter((t) => t.id !== topic.id);
      },
      error: (err) => {
        this.listError = this.errorText(err, 'Could not delete the topic');
      },
    });
  }

  alertPill(alertLevel: AlertLevel): string {
    switch (alertLevel) {
      case 'IMMEDIATE': return 'immediate alerts';
      case 'DAILY': return 'daily digest';
      case 'WEEKLY': return 'weekly digest';
      default: return 'alerts off';
    }
  }

  private parseKeywords(text: string): string[] {
    const out: string[] = [];
    let current = '';
    let inQuotes = false;
    for (const ch of text) {
      if (ch === '"') {
        inQuotes = !inQuotes;
        current += ch;
      } else if (ch === ',' && !inQuotes) {
        out.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
    out.push(current);
    return out.map((k) => k.trim()).filter((k) => k.length > 0);
  }

  private resetForm(): void {
    this.editingId = null;
    this.formError = '';
    this.form = this.emptyForm();
  }

  private emptyForm(): TopicForm {
    return { topic_name: '', keywordsText: '', sources: [], alert_level: 'DAILY' };
  }

  private errorText(err: unknown, fallback: string): string {
    const body = (err as { error?: { error?: string } })?.error;
    return body?.error || fallback;
  }
}
