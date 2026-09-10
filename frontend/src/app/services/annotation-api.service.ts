import { Injectable } from '@angular/core';
import type { EditorAnnotation } from 'ngx-extended-pdf-viewer';

/**
 * The shape a real backend would return. Plain data — no Angular signals,
 * no observable wrapping, no class instances. Easy to swap with an HttpClient
 * call later.
 */
export interface ApiHighlight {
  id: string;
  pageIndex: number;
  rect: [number, number, number, number];
  text: string;
  /**
   * Full serialized annotation data. Typed as the library's `EditorAnnotation`
   * with an open index signature so backend-only fields (structTreeParentId,
   * popupRef, etc.) round-trip without losing data.
   */
  serialized: EditorAnnotation & { [key: string]: unknown };
  createdAt: number;
}

export interface ApiTask {
  id: string;
  name: string;
  highlights: ApiHighlight[];
  createdAt: number;
}

/**
 * Mock backend. Returns hardcoded data with a small async delay to simulate
 * a real network call. Replace the body of `fetchTasks()` with an HttpClient
 * call when the real API is ready.
 */
@Injectable({ providedIn: 'root' })
export class AnnotationApiService {
  async fetchTasks(): Promise<ApiTask[]> {
    await this.delay(150);
    return [HARDCODED_TASK_1, HARDCODED_TASK_2];
  }

  private delay(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }
}

const HARDCODED_TASK_1: ApiTask = {
  id: 'api-task-001',
  name: 'API Task 1 — type information',
  createdAt: Date.now(),
  highlights: [
    {
      id: 'pdfjs_internal_editor_0',
      pageIndex: 0,
      rect: [316.3428018093109, 384.9911906719208, 556.9812079668045, 407.3255910873413],
      text: ' typed languages rely on type informa-\ntion to generate efﬁcient mac',
      serialized: {
        annotationType: 9,
        pageIndex: 0,
        rect: [316.3428018093109, 384.9911906719208, 556.9812079668045, 407.3255910873413],
        rotation: 0,
        structTreeParentId: null,
        popupRef: '',
        color: [255, 255, 152],
        opacity: 1,
        thickness: 12,
        quadPoints: {
          '0': 413.434814453125, '1': 406.507080078125,
          '2': 556.3563842773438, '3': 406.507080078125,
          '4': 413.434814453125, '5': 395.8332214355469,
          '6': 556.3563842773438, '7': 395.8332214355469,
          '8': 317.0118713378906, '9': 396.51702880859375,
          '10': 420.5787353515625, '11': 396.51702880859375,
          '12': 317.0118713378906, '13': 385.8431701660156,
          '14': 420.5787353515625, '15': 385.8431701660156,
        },
        outlines: [[
          412.7940042773195, 397.3463909017279,
          412.7940042773195, 407.3255910873413,
          556.9812079668045, 407.3255910873413,
          556.9812079668045, 394.97039085753414,
          421.2396044934269, 394.97039085753414,
          421.2396044934269, 384.9911906719208,
          316.3428018093109, 384.9911906719208,
          316.3428018093109, 397.3463909017279,
        ]],
        isCopy: true,
      },
      createdAt: 1785497122476,
    },
  ],
};

const HARDCODED_TASK_2: ApiTask = {
  id: 'api-task-002',
  name: 'API Task 2 — generalized machine code',
  createdAt: Date.now(),
  highlights: [
    {
      id: 'pdfjs_internal_editor_1',
      pageIndex: 0,
      rect: [316.3428018093109, 335.17438530921936, 557.0423998832703, 357.5087857246399],
      text: ' information, the compiler\nmust emit slower generalized machine co',
      serialized: {
        annotationType: 9,
        pageIndex: 0,
        rect: [316.3428018093109, 335.17438530921936, 557.0423998832703, 357.5087857246399],
        rotation: 0,
        structTreeParentId: null,
        popupRef: '',
        color: [255, 255, 152],
        opacity: 1,
        thickness: 12,
        quadPoints: {
          '0': 463.2344970703125, '1': 356.690185546875,
          '2': 556.4043579101562, '3': 356.690185546875,
          '4': 463.2344970703125, '5': 346.0163269042969,
          '6': 556.4043579101562, '7': 346.0163269042969,
          '8': 317.0118713378906, '9': 346.70013427734375,
          '10': 469.52374267578125, '11': 346.70013427734375,
          '12': 317.0118713378906, '13': 336.0262756347656,
          '14': 469.52374267578125, '15': 336.0262756347656,
        },
        outlines: [[
          462.6108006388972, 347.5295855390264,
          462.6108006388972, 357.5087857246399,
          557.0423998832703, 357.5087857246399,
          557.0423998832703, 345.1535854948327,
          470.1384005786625, 345.1535854948327,
          470.1384005786625, 335.17438530921936,
          316.3428018093109, 335.17438530921936,
          316.3428018093109, 347.5295855390264,
        ]],
        isCopy: true,
      },
      createdAt: 1785497132580,
    },
  ],
};