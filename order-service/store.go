package main

import (
	"sync"
)

type Order struct {
	ID     string  `json:"id"`
	UserID string  `json:"userId"`
	Amount float64 `json:"amount"`
	Status string  `json:"status"`
}

type OrderStore struct {
	mu     sync.RWMutex
	orders map[string]Order
}

func NewOrderStore() *OrderStore {
	return &OrderStore{
		orders: make(map[string]Order),
	}
}

func (s *OrderStore) Save(order Order) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.orders[order.ID] = order
}

func (s *OrderStore) Get(id string) (Order, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	order, ok := s.orders[id]
	return order, ok
}

func (s *OrderStore) UpdateStatus(id, status string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	order, ok := s.orders[id]
	if !ok {
		return false
	}
	order.Status = status
	s.orders[id] = order
	return true
}
