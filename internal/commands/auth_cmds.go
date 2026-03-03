package commands

import (
	"fmt"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/inder/salvobase/internal/auth"
	"github.com/inder/salvobase/internal/storage"
)

// handleSASLStart handles the "saslStart" command.
// This is step 1 of SCRAM-SHA-256 authentication.
func handleSASLStart(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	mechanism := lookupStringField(cmd, "mechanism")
	if mechanism == "" {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "saslStart: missing 'mechanism' field")
	}

	// Get the payload (client-first-message).
	payloadVal, err := cmd.LookupErr("payload")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "saslStart: missing 'payload' field")
	}

	var payload []byte
	switch payloadVal.Type {
	case bson.TypeBinary:
		_, data, ok := payloadVal.BinaryOK()
		if !ok {
			return nil, storage.Errorf(storage.ErrCodeBadValue, "saslStart: invalid payload binary")
		}
		payload = data
	case bson.TypeString:
		payload = []byte(payloadVal.StringValue())
	default:
		return nil, storage.Errorf(storage.ErrCodeBadValue, "saslStart: payload must be BinData or string")
	}

	// Determine the database. The SASL auth db may come from the "db" field
	// or from the $db field (already stripped into ctx.DB by the connection handler).
	db := lookupStringField(cmd, "db")
	if db == "" {
		db = ctx.DB
	}

	serverFirst, convID, err := ctx.Auth.SASLStart(db, mechanism, payload)
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeAuthenticationFailed, "SASL authentication failed: %v", err)
	}

	return marshalResponse(bson.D{
		{"conversationId", convID},
		{"done", false},
		{"payload", bson.Binary{Data: serverFirst}},
		{"ok", float64(1)},
	}), nil
}

// handleSASLContinue handles the "saslContinue" command.
// This is step 2 (and possibly step 3) of SCRAM-SHA-256 authentication.
func handleSASLContinue(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	convIDVal, err := cmd.LookupErr("conversationId")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "saslContinue: missing 'conversationId' field")
	}
	var convID int32
	switch convIDVal.Type {
	case bson.TypeInt32:
		convID = convIDVal.Int32()
	case bson.TypeInt64:
		convID = int32(convIDVal.Int64())
	default:
		return nil, storage.Errorf(storage.ErrCodeBadValue, "saslContinue: conversationId must be an integer")
	}

	payloadVal, err := cmd.LookupErr("payload")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "saslContinue: missing 'payload' field")
	}

	var payload []byte
	switch payloadVal.Type {
	case bson.TypeBinary:
		_, data, ok := payloadVal.BinaryOK()
		if !ok {
			return nil, storage.Errorf(storage.ErrCodeBadValue, "saslContinue: invalid payload binary")
		}
		payload = data
	case bson.TypeString:
		payload = []byte(payloadVal.StringValue())
	default:
		return nil, storage.Errorf(storage.ErrCodeBadValue, "saslContinue: payload must be BinData or string")
	}

	serverMsg, done, err := ctx.Auth.SASLContinue(convID, payload)
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeAuthenticationFailed, "SASL authentication failed: %v", err)
	}

	// If done and successful, update the connection's auth state.
	// The connection handler will call GetAuthenticatedUser after seeing done=true.
	if done {
		username, db, ok := ctx.Auth.GetAuthenticatedUser(convID)
		if ok {
			ctx.Username = username
			ctx.UserDB = db
			ctx.Auth.RemoveConversation(convID)
		}
	}

	return marshalResponse(bson.D{
		{"conversationId", convID},
		{"done", done},
		{"payload", bson.Binary{Data: serverMsg}},
		{"ok", float64(1)},
	}), nil
}

// handleCreateUser handles the "createUser" command.
func handleCreateUser(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	usernameVal, err := cmd.LookupErr("createUser")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "createUser: missing 'createUser' field")
	}
	username, ok := usernameVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "createUser: username must be a string")
	}

	password := lookupStringField(cmd, "pwd")
	if password == "" {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "createUser: missing 'pwd' field")
	}

	roles := parseRolesFromCmd(cmd, ctx.DB)

	// Check if user already exists.
	exists, err := ctx.Engine.Users().HasUser(ctx.DB, username)
	if err != nil {
		return nil, fmt.Errorf("createUser: %w", err)
	}
	if exists {
		return nil, storage.Errorf(storage.ErrCodeUserAlreadyExists,
			"User \"%s@%s\" already exists", username, ctx.DB)
	}

	storedKey, serverKey, salt, iterCount, err := auth.HashPassword(password)
	if err != nil {
		return nil, fmt.Errorf("createUser: failed to hash password: %w", err)
	}

	user := storage.User{
		ID:        uuid.New().String(),
		DB:        ctx.DB,
		Username:  username,
		StoredKey: storedKey,
		ServerKey: serverKey,
		Salt:      salt,
		IterCount: iterCount,
		Roles:     roles,
	}

	if err := ctx.Engine.Users().CreateUser(user); err != nil {
		return nil, fmt.Errorf("createUser: %w", err)
	}

	return BuildOKResponse(), nil
}

// handleDropUser handles the "dropUser" command.
func handleDropUser(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	usernameVal, err := cmd.LookupErr("dropUser")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "dropUser: missing 'dropUser' field")
	}
	username, ok := usernameVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "dropUser: username must be a string")
	}

	exists, err := ctx.Engine.Users().HasUser(ctx.DB, username)
	if err != nil {
		return nil, fmt.Errorf("dropUser: %w", err)
	}
	if !exists {
		return nil, storage.Errorf(storage.ErrCodeUserNotFound,
			"User \"%s@%s\" not found", username, ctx.DB)
	}

	if err := ctx.Engine.Users().DeleteUser(ctx.DB, username); err != nil {
		return nil, fmt.Errorf("dropUser: %w", err)
	}

	return BuildOKResponse(), nil
}

// handleUpdateUser handles the "updateUser" command.
func handleUpdateUser(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	usernameVal, err := cmd.LookupErr("updateUser")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "updateUser: missing 'updateUser' field")
	}
	username, ok := usernameVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "updateUser: username must be a string")
	}

	update := storage.UserUpdate{}

	// Update password if provided.
	if password := lookupStringField(cmd, "pwd"); password != "" {
		storedKey, serverKey, salt, iterCount, err := auth.HashPassword(password)
		if err != nil {
			return nil, fmt.Errorf("updateUser: failed to hash password: %w", err)
		}
		update.StoredKey = storedKey
		update.ServerKey = serverKey
		update.Salt = salt
		update.IterCount = iterCount
	}

	// Update roles if provided.
	if _, err := cmd.LookupErr("roles"); err == nil {
		update.Roles = parseRolesFromCmd(cmd, ctx.DB)
	}

	if err := ctx.Engine.Users().UpdateUser(ctx.DB, username, update); err != nil {
		return nil, fmt.Errorf("updateUser: %w", err)
	}

	return BuildOKResponse(), nil
}

// handleUsersInfo handles the "usersInfo" command.
func handleUsersInfo(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	usersInfoVal, err := cmd.LookupErr("usersInfo")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "usersInfo: missing 'usersInfo' field")
	}

	var users []storage.User

	switch usersInfoVal.Type {
	case bson.TypeString:
		// Single user by name in current db.
		username := usersInfoVal.StringValue()
		user, ok, err := ctx.Engine.Users().GetUser(ctx.DB, username)
		if err != nil {
			return nil, fmt.Errorf("usersInfo: %w", err)
		}
		if ok {
			users = append(users, user)
		}

	case bson.TypeEmbeddedDocument:
		// {"user": "name", "db": "dbname"}
		spec, _ := usersInfoVal.DocumentOK()
		username := lookupStringField(spec, "user")
		db := lookupStringField(spec, "db")
		if db == "" {
			db = ctx.DB
		}
		user, ok, err := ctx.Engine.Users().GetUser(db, username)
		if err != nil {
			return nil, fmt.Errorf("usersInfo: %w", err)
		}
		if ok {
			users = append(users, user)
		}

	case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble:
		// 1 = all users in current db.
		var listErr error
		users, listErr = ctx.Engine.Users().ListUsers(ctx.DB)
		if listErr != nil {
			return nil, fmt.Errorf("usersInfo: %w", listErr)
		}

	case bson.TypeArray:
		// Array of user specs.
		arr, _ := usersInfoVal.ArrayOK()
		vals, _ := arr.Values()
		for _, elem := range vals {
			switch elem.Type {
			case bson.TypeString:
				user, ok, err := ctx.Engine.Users().GetUser(ctx.DB, elem.StringValue())
				if err == nil && ok {
					users = append(users, user)
				}
			case bson.TypeEmbeddedDocument:
				spec, _ := elem.DocumentOK()
				username := lookupStringField(spec, "user")
				db := lookupStringField(spec, "db")
				if db == "" {
					db = ctx.DB
				}
				user, ok, err := ctx.Engine.Users().GetUser(db, username)
				if err == nil && ok {
					users = append(users, user)
				}
			}
		}
	}

	userDocs := make(bson.A, 0, len(users))
	for _, u := range users {
		rolesDocs := make(bson.A, 0, len(u.Roles))
		for _, r := range u.Roles {
			rolesDocs = append(rolesDocs, bson.D{
				{"role", r.Role},
				{"db", r.DB},
			})
		}
		userDocs = append(userDocs, bson.D{
			{"_id", u.DB + "." + u.Username},
			{"userId", u.ID},
			{"user", u.Username},
			{"db", u.DB},
			{"roles", rolesDocs},
		})
	}

	return marshalResponse(bson.D{
		{"users", userDocs},
		{"ok", float64(1)},
	}), nil
}

// handleGrantRolesToUser handles the "grantRolesToUser" command.
func handleGrantRolesToUser(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	usernameVal, err := cmd.LookupErr("grantRolesToUser")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "grantRolesToUser: missing field")
	}
	username, ok := usernameVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "grantRolesToUser: username must be a string")
	}

	newRoles := parseRolesFromCmd(cmd, ctx.DB)

	user, found, err := ctx.Engine.Users().GetUser(ctx.DB, username)
	if err != nil || !found {
		return nil, storage.Errorf(storage.ErrCodeUserNotFound, "user not found: %s", username)
	}

	// Merge new roles, avoiding duplicates.
	existingRoles := user.Roles
	for _, newRole := range newRoles {
		found := false
		for _, existing := range existingRoles {
			if existing.Role == newRole.Role && existing.DB == newRole.DB {
				found = true
				break
			}
		}
		if !found {
			existingRoles = append(existingRoles, newRole)
		}
	}

	update := storage.UserUpdate{Roles: existingRoles}
	if err := ctx.Engine.Users().UpdateUser(ctx.DB, username, update); err != nil {
		return nil, fmt.Errorf("grantRolesToUser: %w", err)
	}

	return BuildOKResponse(), nil
}

// handleRevokeRolesFromUser handles the "revokeRolesFromUser" command.
func handleRevokeRolesFromUser(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	usernameVal, err := cmd.LookupErr("revokeRolesFromUser")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "revokeRolesFromUser: missing field")
	}
	username, ok := usernameVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "revokeRolesFromUser: username must be a string")
	}

	rolesToRevoke := parseRolesFromCmd(cmd, ctx.DB)

	user, found, err := ctx.Engine.Users().GetUser(ctx.DB, username)
	if err != nil || !found {
		return nil, storage.Errorf(storage.ErrCodeUserNotFound, "user not found: %s", username)
	}

	// Remove revoked roles.
	var remainingRoles []storage.Role
	for _, existing := range user.Roles {
		revoke := false
		for _, r := range rolesToRevoke {
			if existing.Role == r.Role && existing.DB == r.DB {
				revoke = true
				break
			}
		}
		if !revoke {
			remainingRoles = append(remainingRoles, existing)
		}
	}

	update := storage.UserUpdate{Roles: remainingRoles}
	if err := ctx.Engine.Users().UpdateUser(ctx.DB, username, update); err != nil {
		return nil, fmt.Errorf("revokeRolesFromUser: %w", err)
	}

	return BuildOKResponse(), nil
}

// parseRolesFromCmd parses the "roles" array from a command document.
// Each role can be a string (shorthand for role in current db) or a document
// {"role": "roleName", "db": "dbName"}.
func parseRolesFromCmd(cmd bson.Raw, defaultDB string) []storage.Role {
	rolesVal, err := cmd.LookupErr("roles")
	if err != nil {
		return nil
	}
	arr, ok := rolesVal.ArrayOK()
	if !ok {
		return nil
	}
	vals, err := arr.Values()
	if err != nil {
		return nil
	}

	roles := make([]storage.Role, 0, len(vals))
	for _, elem := range vals {
		switch elem.Type {
		case bson.TypeString:
			roles = append(roles, storage.Role{
				Role: elem.StringValue(),
				DB:   defaultDB,
			})
		case bson.TypeEmbeddedDocument:
			doc, _ := elem.DocumentOK()
			role := lookupStringField(doc, "role")
			db := lookupStringField(doc, "db")
			if db == "" {
				db = defaultDB
			}
			if role != "" {
				roles = append(roles, storage.Role{Role: role, DB: db})
			}
		}
	}
	return roles
}
