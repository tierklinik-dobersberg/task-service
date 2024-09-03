package inmem

import (
	"sync"

	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	"github.com/tierklinik-dobersberg/task-service/internal/repo"
)

type Repository struct {
	l sync.RWMutex

	customers map[string]*customerv1.Customer
	states    map[string][]*customerv1.ImportState

	locks map[string]string
}

func New() *Repository {
	return &Repository{
		customers: make(map[string]*customerv1.Customer),
		states:    make(map[string][]*customerv1.ImportState),
		locks:     make(map[string]string),
	}
}

var _ repo.Backend = (*Repository)(nil)
