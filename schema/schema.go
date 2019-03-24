package schema

// BookingRequest represents request for booking message
type BookingRequest struct {
	RequestID string `json:"requestId"`
	Name      string `json:"name"`
	CitizenID string `json:"citizenId"`
}

// BookingResponse represents response from booking system for a booking request
type BookingResponse struct {
	RequestID string `json:"requestId"`
	BookingID string `json:"bookingId"`
	Error     string `json:"error"`
}
