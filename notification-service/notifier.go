package main

import (
	"fmt"
	"log"
	"time"
)

type Notifier struct{}

func NewNotifier() *Notifier {
	return &Notifier{}
}

func (n *Notifier) Notify(event PaymentEvent) {
	switch event.Type {
	case "PaymentCompleted":
		n.sendSuccess(event)
	case "PaymentFailed":
		n.sendFailure(event)
	default:
		log.Printf("[Notifier] Unknown event type: %s", event.Type)
	}
}

func (n *Notifier) sendSuccess(event PaymentEvent) {
	subject := "Payment Successful"
	body := fmt.Sprintf(
		"Hello (userID: %s),\n\nYour payment of $%.2f for order %s was completed successfully.\n\nThank you for your purchase!",
		event.UserID, event.Amount, event.OrderID,
	)
	n.send(event.UserID, subject, body)
}

func (n *Notifier) sendFailure(event PaymentEvent) {
	subject := "Payment Failed"
	body := fmt.Sprintf(
		"Hello (userID: %s),\n\nUnfortunately, your payment of $%.2f for order %s has failed.\nReason: %s\n\nPlease try again.",
		event.UserID, event.Amount, event.OrderID, event.Message,
	)
	n.send(event.UserID, subject, body)
}

func (n *Notifier) send(userID, subject, body string) {
	// In production, replace this with a real email/SMS provider (e.g. SendGrid, Twilio).
	log.Printf("[Notifier] %s | To: user=%s | Time: %s\n--- Message ---\n%s\n---------------",
		subject, userID, time.Now().Format(time.RFC3339), body,
	)
}
