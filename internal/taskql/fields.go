package taskql

type Field string

const (
	FieldStatus    = Field("status")
	FieldAssignee  = Field("assignee")
	FieldTag       = Field("tag")
	FieldCompleted = Field("completed")
	FieldCreator   = Field("creator")
	FieldPriority  = Field("priority")
	FieldDueBefore = Field("due_before")
	FieldDueAfter  = Field("due_after")
	FieldDueAt     = Field("due_at")
)

var allFields = []Field{
	FieldStatus,
	FieldAssignee,
	FieldTag,
	FieldCompleted,
	FieldCreator,
	FieldPriority,
	FieldDueBefore,
	FieldDueAfter,
	FieldDueAt,
}

func (f Field) IsValid() bool {
	for _, e := range allFields {
		if e == f {
			return true
		}
	}

	return false
}
