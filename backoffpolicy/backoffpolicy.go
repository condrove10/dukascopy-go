package backoffpolicy

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/go-playground/validator/v10"
)

type Strategy string

const (
	StrategyLinear      Strategy = "Linear"
	StrategyExponential Strategy = "Exponential"
)

func ValidateStrategy(s Strategy) error {
	allowedStrategies := []Strategy{StrategyLinear, StrategyExponential}

	for _, allowed := range allowedStrategies {
		if s == allowed {
			return nil
		}
	}
	return errors.New("invalid strategy: " + string(s))
}

func BackoffPolicy(strategy Strategy, attempts int, delay time.Duration, policy func(attempt int) error) error {
	if err := ValidateStrategy(strategy); err != nil {
		return fmt.Errorf("strategy validation failed: %w", err)
	}

	if err := validator.New().Var(attempts, "required,gt=0,lte=100000"); err != nil {
		return fmt.Errorf("attempts validation failed: %w", err)
	}
	var err error

	for attempt := 0; attempt < attempts; attempt++ {
		err = policy(attempt)
		if err == nil {
			return nil
		}

		switch strategy {
		case StrategyExponential:
			time.Sleep(delay * time.Duration(math.Pow(2, float64(attempt))))
		case StrategyLinear:
			time.Sleep(delay)
		default:
			return fmt.Errorf("invalid backoff strategy")
		}
	}

	return fmt.Errorf("backoff policy exhausted: %w", err)
}
