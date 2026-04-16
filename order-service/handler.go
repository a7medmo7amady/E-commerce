package main

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type CreateOrderRequest struct {
	UserID string  `json:"userId" binding:"required"`
	Amount float64 `json:"amount" binding:"required,gt=0"`
}

func RegisterRoutes(r *gin.Engine, store *OrderStore, producer *KafkaProducer) {
	r.POST("/orders", func(c *gin.Context) {
		var req CreateOrderRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": err.Error(),
			})
			return
		}

		orderID := generateID("ord")
		cmdID := generateID("cmd")

		order := Order{
			ID:     orderID,
			UserID: req.UserID,
			Amount: req.Amount,
			Status: "PENDING",
		}

		store.Save(order)

		cmd := map[string]any{
			"commandId": cmdID,
			"orderId":   orderID,
			"userId":    req.UserID,
			"amount":    req.Amount,
			"type":      "ProcessPayment",
			"createdAt": time.Now().UTC().Format(time.RFC3339),
		}

		if err := producer.Publish("payment-commands", orderID, cmd); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to publish payment command",
			})
			return
		}

		c.JSON(http.StatusAccepted, gin.H{
			"message": "order created; payment processing started asynchronously",
			"order":   order,
		})
	})

	r.GET("/orders/:id", func(c *gin.Context) {
		id := c.Param("id")
		order, ok := store.Get(id)
		if !ok {
			c.JSON(http.StatusNotFound, gin.H{"error": "order not found"})
			return
		}
		c.JSON(http.StatusOK, order)
	})
}

