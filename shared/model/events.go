type Order struct {
	ID     string  `json:"id"`
	UserID string  `json:"userId"`
	Amount float64 `json:"amount"`
	Status string  `json:"status"`
}

type ProcessPaymentCommand struct {
	CommandID string  `json:"commandId"`
	OrderID   string  `json:"orderId"`
	UserID    string  `json:"userId"`
	Amount    float64 `json:"amount"`
	Type      string  `json:"type"` // "ProcessPayment"
}

type PaymentEvent struct {
	EventID  string  `json:"eventId"`
	OrderID  string  `json:"orderId"`
	UserID   string  `json:"userId"`
	Amount   float64 `json:"amount"`
	Type     string  `json:"type"` // "PaymentCompleted" or "PaymentFailed"
	Message  string  `json:"message"`
}
