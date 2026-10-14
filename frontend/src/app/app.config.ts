import { APP_INITIALIZER, ApplicationConfig } from '@angular/core';
import { provideRouter, withInMemoryScrolling } from '@angular/router';
import { provideHttpClient, withInterceptors } from '@angular/common/http';

import { routes } from './app.routes';
import { authInterceptor } from './auth.interceptor';
import { CurrentPersonaService } from './services/current-persona.service';

export const appConfig: ApplicationConfig = {
  providers: [
    provideRouter(
      routes,
      // Preserve the URL fragment on navigation and let `ActivatedRoute.fragment`
      // emit it — without these, deep-links like
      // /taskview?...#comment-<uuid> lose their hash on first navigation
      // and `route.fragment.subscribe(...)` never fires.
      withInMemoryScrolling({
        anchorScrolling: 'enabled',
        scrollPositionRestoration: 'enabled',
      }),
    ),
    provideHttpClient(withInterceptors([authInterceptor])),

    // Hydrate CurrentPersonaService from localStorage at app start.
    // No HTTP — fast, deterministic. Login flow populates fresh /me data
    // via CurrentPersonaService.setFromMe() in fetchUserAndGoHome().
    {
      provide: APP_INITIALIZER,
      useFactory: (persona: CurrentPersonaService) => () => {
        persona.hydrateFromStorage();
        return Promise.resolve();
      },
      deps: [CurrentPersonaService],
      multi: true,
    },
  ],
};