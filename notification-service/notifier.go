package main

import "log"

type Notifier struct{}

func NewNotifier() *Notifier {
	return &Notifier{}
}

func (n *Notifier) Notify(event PaymentEvent) {
	switch event.Type {
	case "PaymentCompleted":
		log.Println("Notification: Payment Success")
	case "PaymentFailed":
		log.Println("Notification: Payment Failed")
	default:
		log.Printf("Notification: Unknown event type: %s", event.Type)
	}
}
