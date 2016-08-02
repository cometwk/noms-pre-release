// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package functions

import "sync"

// Runs all functions in |fs| in parallel, and returns when all functions have returned.
func All(fs ...func()) {
	finChan := make(chan struct{})
	errChan := make(chan interface{})

	wg := &sync.WaitGroup{}
	wg.Add(len(fs))
	go func() {
		wg.Wait()
		finChan <- struct{}{}
	}()

	for _, f_ := range fs {
		f := f_
		go func() {
			defer func() {
				if r := recover(); r == nil {
					wg.Done()
				} else {
					errChan <- r
				}
			}()
			f()
		}()
	}

	select {
	case <-finChan:
	case err := <-errChan:
		panic(err)
	}
}
