package repo

import "errors"

var (
	ErrCustomerNotFound = errors.New("customer not found")
	ErrCustomerLocked   = errors.New("customer already locked")
)
