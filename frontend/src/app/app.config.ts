import { ApplicationConfig } from '@angular/core';
import { provideRouter, withInMemoryScrolling } from '@angular/router';
import { provideHttpClient, withInterceptors } from '@angular/common/http';

import { routes } from './app.routes';
import { authInterceptor } from './auth.interceptor';

export const appConfig: ApplicationConfig = {
  providers: [
    provideRouter(
      routes,
      // Preserve the URL fragment on navigation and let `ActivatedRoute.fragment`
      // emit it — without these, deep-links like
      // /taskview?...#comment-<uuid> lose their hash on initial load
      // and `route.fragment.subscribe(...)` never fires.
      withInMemoryScrolling({
        anchorScrolling: 'enabled',
        scrollPositionRestoration: 'enabled',
      }),
    ),
    provideHttpClient(withInterceptors([authInterceptor]))
  ]
};
