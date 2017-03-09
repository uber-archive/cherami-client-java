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

import com.uber.cherami.ConsumerMessage;

/**
 * An object that is returned to the client. It wraps the ConsumerMessage and the CheramiAcknowledger,
 * which is the outputHostConnection used to ack/nack the messages.
 */
public class CheramiDeliveryImpl implements CheramiDelivery {
    private final ConsumerMessage message;
    private final CheramiAcknowledger acknowledger;

    /**
     * Create a CheramiDelivery object
     *
     * @param msg          The consumerMessage received from the OutputHost
     * @param acknowledger The OutputHostConnection that will be used to send the ack/nack to the outputHost
     */
    public CheramiDeliveryImpl(ConsumerMessage msg, CheramiAcknowledger acknowledger) {
        this.message = msg;
        this.acknowledger = acknowledger;
    }

    public ConsumerMessage getMessage() {
        return message;
    }

    /**
     * This token is parsed in the constructor of Consumer.DeliveryID
     *
     * @return Delivery token represented by acknowledgerId|AckId
     */
    public String getDeliveryToken() {
        return String.format("%s|%s", acknowledger.getAcknowledgerID(), message.getAckId());
    }

    /**
     * Ack this message
     */
    public void ack() {
        acknowledger.ack(message.getAckId());
    }

    /**
     * Nack this message
     */
    public void nack() {
        acknowledger.nack(message.getAckId());
    }
}
