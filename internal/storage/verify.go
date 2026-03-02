package storage

// Compile-time interface checks.
var (
	_ Engine      = (*BBoltEngine)(nil)
	_ Collection  = (*bboltCollection)(nil)
	_ Cursor      = (*sliceCursor)(nil)
	_ CursorStore = (*cursorStore)(nil)
	_ UserStore   = (*userStore)(nil)
)
