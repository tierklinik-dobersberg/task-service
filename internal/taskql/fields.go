package taskql

type Field string

const (
	FieldStatus    = Field("status")
	FieldAssignee  = Field("assignee")
	FieldTag       = Field("tag")
	FieldCompleted = Field("completed")
	FieldCreator   = Field("creator")
	FieldPriority  = Field("priority")
)

var allFields = []Field{
	FieldStatus,
	FieldAssignee,
	FieldTag,
	FieldCompleted,
	FieldCreator,
	FieldPriority,
}

func (f Field) IsValid() bool {
	for _, e := range allFields {
		if e == f {
			return true
		}
	}

	return false
}
