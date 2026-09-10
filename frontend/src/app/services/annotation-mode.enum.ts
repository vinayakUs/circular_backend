/**
 * Local mirror of ngx-extended-pdf-viewer's AnnotationEditorType numeric values,
 * exposed under friendly names so component code reads naturally.
 *
 * Underlying values come from the library's `AnnotationEditorType` enum and are
 * emitted on the `(annotationEditorModeChanged)` event of `<ngx-extended-pdf-viewer>`.
 */
export enum AnnotationMode {
  DISABLE = -1,
  NONE = 0,
  FREETEXT = 3,
  HIGHLIGHT = 9,
  STAMP = 13,
  INK = 15,
  POPUP = 16,
  SIGNATURE = 101,
  COMMENT = 102,
}