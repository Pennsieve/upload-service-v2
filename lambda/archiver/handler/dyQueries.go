package handler

import (
	dyQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/dydb"
)

// ServiceDyQueries is the Service Queries Struct embedding the shared Queries struct
type ServiceDyQueries struct {
	*dyQueries.Queries
	db dyQueries.DB
}

// NewServiceDyQueries returns a new instance of an ServiceDyQueries object
func NewServiceDyQueries(db dyQueries.DB) *ServiceDyQueries {
	q := dyQueries.New(db)
	return &ServiceDyQueries{
		q,
		db,
	}
}
