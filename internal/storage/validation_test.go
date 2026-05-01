package storage

import (
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// isValidationError checks if an error is a document validation failure.
func isValidationError(err error) bool {
	var me *MongoError
	return errors.As(err, &me) && me.Code == ErrCodeDocumentValidationFailure
}

// TestValidation_InsertRejectedByValidator verifies that inserts are rejected
// when the document fails the collection's validator (validationAction: error).
func TestValidation_InsertRejectedByValidator(t *testing.T) {
	e := newTestEngine(t)
	// Create collection with validator requiring "email" field.
	validator := mustMarshal(bson.D{{Key: "email", Value: bson.D{{Key: "$exists", Value: true}}}})
	opts := CreateCollectionOptions{
		Validator:        validator,
		ValidationLevel:  "strict",
		ValidationAction: "error",
	}
	if err := e.CreateCollection("testdb", "users", opts); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	coll, err := e.Collection("testdb", "users")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	// Valid doc: should succeed.
	validDoc := mustMarshal(bson.D{{Key: "email", Value: "a@b.com"}, {Key: "name", Value: "Alice"}})
	if _, err := coll.InsertOne(validDoc); err != nil {
		t.Fatalf("InsertOne valid: %v", err)
	}

	// Invalid doc: missing email → should fail.
	invalidDoc := mustMarshal(bson.D{{Key: "name", Value: "Bob"}})
	_, err = coll.InsertOne(invalidDoc)
	if err == nil {
		t.Fatal("expected validation error for invalid doc, got nil")
	}
	if !isValidationError(err) {
		t.Fatalf("expected document validation failure error, got: %v", err)
	}
}

// TestValidation_UpdateRejectedByValidator verifies that updates are rejected
// when the result would fail the validator.
func TestValidation_UpdateRejectedByValidator(t *testing.T) {
	e := newTestEngine(t)
	validator := mustMarshal(bson.D{{Key: "email", Value: bson.D{{Key: "$exists", Value: true}}}})
	opts := CreateCollectionOptions{
		Validator:        validator,
		ValidationLevel:  "strict",
		ValidationAction: "error",
	}
	if err := e.CreateCollection("testdb", "users", opts); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	coll, err := e.Collection("testdb", "users")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	// Insert a valid document first.
	doc := mustMarshal(bson.D{{Key: "email", Value: "a@b.com"}, {Key: "name", Value: "Alice"}})
	if _, err := coll.InsertOne(doc); err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	// Update to remove email → should fail.
	filter := mustMarshal(bson.D{{Key: "name", Value: "Alice"}})
	update := mustMarshal(bson.D{{Key: "$unset", Value: bson.D{{Key: "email", Value: ""}}}})
	_, err = coll.UpdateOne(filter, update, UpdateOptions{})
	if err == nil {
		t.Fatal("expected validation error for update removing email, got nil")
	}
	if !isValidationError(err) {
		t.Fatalf("expected document validation failure, got: %v", err)
	}
}

// TestValidation_ModerateSkipsAlreadyInvalid verifies that validationLevel
// "moderate" allows updates to documents that were already invalid.
func TestValidation_ModerateSkipsAlreadyInvalid(t *testing.T) {
	e := newTestEngine(t)
	// Create collection without validator first, insert an "invalid" doc.
	if err := e.CreateCollection("testdb", "users", CreateCollectionOptions{}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	coll, err := e.Collection("testdb", "users")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}
	invalidDoc := mustMarshal(bson.D{{Key: "name", Value: "NoEmail"}})
	if _, err := coll.InsertOne(invalidDoc); err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	// Now add validator via collMod (simulated by UpdateCollectionOptions).
	validator := mustMarshal(bson.D{{Key: "email", Value: bson.D{{Key: "$exists", Value: true}}}})
	if err := e.UpdateCollectionOptions("testdb", "users", validator, "moderate", "error"); err != nil {
		t.Fatalf("UpdateCollectionOptions: %v", err)
	}

	// Updating the already-invalid doc should succeed under "moderate".
	filter := mustMarshal(bson.D{{Key: "name", Value: "NoEmail"}})
	update := mustMarshal(bson.D{{Key: "$set", Value: bson.D{{Key: "age", Value: 30}}}})
	result, err := coll.UpdateOne(filter, update, UpdateOptions{})
	if err != nil {
		t.Fatalf("UpdateOne moderate on invalid doc: %v", err)
	}
	if result.ModifiedCount != 1 {
		t.Fatalf("expected ModifiedCount=1, got %d", result.ModifiedCount)
	}

	// But inserting a new invalid doc should still fail.
	newInvalid := mustMarshal(bson.D{{Key: "name", Value: "StillNoEmail"}})
	_, err = coll.InsertOne(newInvalid)
	if err == nil {
		t.Fatal("expected validation error for new insert under moderate, got nil")
	}
}

// TestValidation_WarnAllowsWrite verifies that validationAction "warn" allows
// the write to proceed even when the document fails validation.
func TestValidation_WarnAllowsWrite(t *testing.T) {
	e := newTestEngine(t)
	validator := mustMarshal(bson.D{{Key: "email", Value: bson.D{{Key: "$exists", Value: true}}}})
	opts := CreateCollectionOptions{
		Validator:        validator,
		ValidationLevel:  "strict",
		ValidationAction: "warn",
	}
	if err := e.CreateCollection("testdb", "users", opts); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	coll, err := e.Collection("testdb", "users")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	// Insert invalid doc → should succeed with warn.
	invalidDoc := mustMarshal(bson.D{{Key: "name", Value: "Bob"}})
	if _, err := coll.InsertOne(invalidDoc); err != nil {
		t.Fatalf("InsertOne with warn: %v", err)
	}
}

// TestValidation_LevelOff verifies that validationLevel "off" skips validation.
func TestValidation_LevelOff(t *testing.T) {
	e := newTestEngine(t)
	validator := mustMarshal(bson.D{{Key: "email", Value: bson.D{{Key: "$exists", Value: true}}}})
	opts := CreateCollectionOptions{
		Validator:        validator,
		ValidationLevel:  "off",
		ValidationAction: "error",
	}
	if err := e.CreateCollection("testdb", "users", opts); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	coll, err := e.Collection("testdb", "users")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	// Insert invalid doc → should succeed with level=off.
	invalidDoc := mustMarshal(bson.D{{Key: "name", Value: "Bob"}})
	if _, err := coll.InsertOne(invalidDoc); err != nil {
		t.Fatalf("InsertOne with level=off: %v", err)
	}
}

// TestValidation_BypassDocumentValidation verifies that the
// BypassDocumentValidation flag skips validation.
func TestValidation_BypassDocumentValidation(t *testing.T) {
	e := newTestEngine(t)
	validator := mustMarshal(bson.D{{Key: "email", Value: bson.D{{Key: "$exists", Value: true}}}})
	opts := CreateCollectionOptions{
		Validator:        validator,
		ValidationLevel:  "strict",
		ValidationAction: "error",
	}
	if err := e.CreateCollection("testdb", "users", opts); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	coll, err := e.Collection("testdb", "users")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	// Insert invalid doc with bypass → should succeed.
	invalidDoc := mustMarshal(bson.D{{Key: "name", Value: "Bob"}})
	_, err = coll.InsertMany([]bson.Raw{invalidDoc}, InsertOptions{
		Ordered:                  true,
		BypassDocumentValidation: true,
	})
	if err != nil {
		t.Fatalf("InsertMany with bypass: %v", err)
	}
}

// TestValidation_CollMod verifies that UpdateCollectionOptions changes the
// active validator on an existing collection.
func TestValidation_CollMod(t *testing.T) {
	e := newTestEngine(t)
	// Create collection without validator.
	if err := e.CreateCollection("testdb", "users", CreateCollectionOptions{}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	coll, err := e.Collection("testdb", "users")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	// Insert a doc without email → should succeed (no validator).
	doc := mustMarshal(bson.D{{Key: "name", Value: "Alice"}})
	if _, err := coll.InsertOne(doc); err != nil {
		t.Fatalf("InsertOne before collMod: %v", err)
	}

	// Add validator via collMod.
	validator := mustMarshal(bson.D{{Key: "email", Value: bson.D{{Key: "$exists", Value: true}}}})
	if err := e.UpdateCollectionOptions("testdb", "users", validator, "strict", "error"); err != nil {
		t.Fatalf("UpdateCollectionOptions: %v", err)
	}

	// Now insert without email should fail.
	doc2 := mustMarshal(bson.D{{Key: "name", Value: "Bob"}})
	_, err = coll.InsertOne(doc2)
	if err == nil {
		t.Fatal("expected validation error after collMod, got nil")
	}
	if !isValidationError(err) {
		t.Fatalf("expected document validation failure, got: %v", err)
	}

	// Insert with email should succeed.
	doc3 := mustMarshal(bson.D{{Key: "name", Value: "Charlie"}, {Key: "email", Value: "c@d.com"}})
	if _, err := coll.InsertOne(doc3); err != nil {
		t.Fatalf("InsertOne valid after collMod: %v", err)
	}
}

// TestValidation_ReplaceRejected verifies that ReplaceOne is rejected when
// the replacement fails the validator.
func TestValidation_ReplaceRejected(t *testing.T) {
	e := newTestEngine(t)
	validator := mustMarshal(bson.D{{Key: "email", Value: bson.D{{Key: "$exists", Value: true}}}})
	opts := CreateCollectionOptions{
		Validator:        validator,
		ValidationLevel:  "strict",
		ValidationAction: "error",
	}
	if err := e.CreateCollection("testdb", "users", opts); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	coll, err := e.Collection("testdb", "users")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	// Insert valid doc.
	doc := mustMarshal(bson.D{{Key: "email", Value: "a@b.com"}, {Key: "name", Value: "Alice"}})
	if _, err := coll.InsertOne(doc); err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	// Replace with invalid doc (no email).
	filter := mustMarshal(bson.D{{Key: "name", Value: "Alice"}})
	replacement := mustMarshal(bson.D{{Key: "name", Value: "Alice Updated"}})
	_, err = coll.ReplaceOne(filter, replacement, UpdateOptions{})
	if err == nil {
		t.Fatal("expected validation error for replace, got nil")
	}
	if !isValidationError(err) {
		t.Fatalf("expected document validation failure, got: %v", err)
	}
}

// TestValidation_JsonSchema verifies that $jsonSchema validators work.
func TestValidation_JsonSchema(t *testing.T) {
	e := newTestEngine(t)
	validator := mustMarshal(bson.D{{Key: "$jsonSchema", Value: bson.D{
		{Key: "required", Value: bson.A{"email"}},
		{Key: "properties", Value: bson.D{
			{Key: "email", Value: bson.D{{Key: "bsonType", Value: "string"}}},
		}},
	}}})
	opts := CreateCollectionOptions{
		Validator:        validator,
		ValidationLevel:  "strict",
		ValidationAction: "error",
	}
	if err := e.CreateCollection("testdb", "users", opts); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	coll, err := e.Collection("testdb", "users")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	// Valid: has string email.
	validDoc := mustMarshal(bson.D{{Key: "email", Value: "a@b.com"}})
	if _, err := coll.InsertOne(validDoc); err != nil {
		t.Fatalf("InsertOne valid: %v", err)
	}

	// Invalid: missing email.
	invalidDoc := mustMarshal(bson.D{{Key: "name", Value: "Bob"}})
	_, err = coll.InsertOne(invalidDoc)
	if err == nil {
		t.Fatal("expected validation error for missing email, got nil")
	}

	// Invalid: email is wrong type.
	wrongType := mustMarshal(bson.D{{Key: "email", Value: 123}})
	_, err = coll.InsertOne(wrongType)
	if err == nil {
		t.Fatal("expected validation error for wrong email type, got nil")
	}
}
