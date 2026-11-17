import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../environments/environment';

export type AlertLevel = 'IMMEDIATE' | 'DAILY' | 'WEEKLY' | 'OFF';

export interface Topic {
  id: string;
  topic_name: string;
  keywords: string[];
  sources: string[];
  alert_level: AlertLevel;
  created_at: string;
  updated_at: string;
}

export interface TopicPayload {
  topic_name?: string;
  keywords?: string[];
  sources?: string[];
  alert_level?: AlertLevel;
}

export type DigestFrequency = 'DAILY' | 'WEEKLY' | 'NONE';

export interface NotificationPrefs {
  digest_frequency: DigestFrequency;
}

@Injectable({ providedIn: 'root' })
export class TopicsApiService {
  constructor(private http: HttpClient) {}

  getTopics(): Observable<Topic[]> {
    return this.http.get<Topic[]>(`${environment.apiUrl}/api/topics`);
  }

  createTopic(payload: TopicPayload): Observable<Topic> {
    return this.http.post<Topic>(`${environment.apiUrl}/api/topics`, payload);
  }

  updateTopic(id: string, payload: TopicPayload): Observable<Topic> {
    return this.http.put<Topic>(`${environment.apiUrl}/api/topics/${id}`, payload);
  }

  deleteTopic(id: string): Observable<void> {
    return this.http.delete<void>(`${environment.apiUrl}/api/topics/${id}`);
  }

  getNotificationPrefs(): Observable<NotificationPrefs> {
    return this.http.get<NotificationPrefs>(`${environment.apiUrl}/api/notification-prefs`);
  }

  updateNotificationPrefs(digest_frequency: DigestFrequency): Observable<NotificationPrefs> {
    return this.http.put<NotificationPrefs>(`${environment.apiUrl}/api/notification-prefs`, {
      digest_frequency,
    });
  }
}
