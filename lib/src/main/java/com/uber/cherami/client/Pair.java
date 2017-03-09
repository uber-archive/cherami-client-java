/*******************************************************************************
 *  Copyright (c) 2017 Uber Technologies, Inc.
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 *******************************************************************************/
package com.uber.cherami.client;

/**
 * Pair represents a two element tuple.
 * Immutable and thread safe.
 */
public class Pair<F,S> {

	/** first element in the tuple */
	private final F first;
	/** second element in the tuple */
	private final S second;

	/** 
	 * Creates and returns a Pair object 
	 *
	 * @param first
	 *     First element of tuple, type F
	 * @param second
	 *     Second element of tuple, type S
	 */
	public Pair(F first, S second) {
		this.first = first;
		this.second = second;
	}

	/**
	 * Returns the first element.
	 */
	public F getFirst() {
		return this.first;
	}

	/**
	 * Returns the second element.
	 */
	public S getSecond() {
		return this.second;
	}
}