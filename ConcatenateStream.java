/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

import com.google.common.collect.Lists;


/**
*  Example Stream for Lucene / Solr Revolution 2016
*  
*  Shows how to make decisions based on the values in each read tuple
**/

public class ConcatenateStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private PushBackStream incomingStream;
  private String leftFieldName;
  private String rightFieldName;

  public ConcatenateStream(TupleStream incomingStream, String leftFieldName, String rightFieldName) throws IOException {
    init(incomingStream, leftFieldName, rightFieldName);
  }
  
  /**
   * Required constructor accepting a StreamExpression and StreamFactory. The factory is used to build a valid
   * TupleNumberStream object out of the passed in expression. If any errors are found we will fail fast and
   * throw an appropriate exception.
   */
  public ConcatenateStream(StreamExpression expression, StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    
    String leftFieldName = factory.getStringOperand(expression, "left");
    String rightFieldName = factory.getStringOperand(expression, "right");
    if(null == leftFieldName || null == rightFieldName){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expected leftFieldName and rightFieldName but didn't find one", expression));
    }
    
    // validate expression contains only what we want.
    if(expression.getParameters().size() != streamExpressions.size() + 2){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }
    
    // construct the incoming stream
    TupleStream stream = factory.constructStream(streamExpressions.get(0));
    init(stream, leftFieldName, rightFieldName);
  }
  
  private void init(TupleStream incomingStream, String leftFieldName, String rightFieldName) throws IOException{
    this.incomingStream = new PushBackStream(incomingStream);
    this.leftFieldName = leftFieldName;
    this.rightFieldName = rightFieldName;
  }
  
  /**
   * Open the incoming stream.
   */
  public void open() throws IOException {
    incomingStream.open();
  }

  /**
   * Close the incoming stream.
   */
  public void close() throws IOException {
    incomingStream.close();
  }
  
  /**
   * Pass the context down to the incoming stream. 
   * There's nothing in it that this stream might care about.
   */
  public void setStreamContext(StreamContext context) {
    this.incomingStream.setStreamContext(context);
  }

  /**
   * Return list of all incoming streams
   */
  public List<TupleStream> children() {
    return Lists.newArrayList(incomingStream);
  }
  
  /**
   * As of now not used by anything. Just return 0. 
   * Intention is to allow us to set a cost of executing this stream to support
   * possible stream optimizations.
   */
  public int getCost() {
    return 0;
  }
  
  /**
   * Return a comparator describing the sort order of the tuples coming out
   * of this stream. Because we don't modify the incoming order we can just
   * return that
   */
  public StreamComparator getStreamSort(){
    return incomingStream.getStreamSort();
  }
  
  /**
   * Convert this object into a valid StreamExpression
   */
  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }
  
  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
        
    if(includeStreams){
      if(incomingStream instanceof Expressible){
        expression.addParameter(((Expressible)incomingStream).toExpression(factory));
      }
      else{
        throw new IOException("This TupleNumberStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    }
    else{
      expression.addParameter("<stream>");
    }
    
    expression.addParameter(new StreamExpressionNamedParameter("left", leftFieldName));
    expression.addParameter(new StreamExpressionNamedParameter("right", rightFieldName));
    
    return expression;   
  }
  
  /**
   * Create an explanation of this stream object
   */
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    return new StreamExplanation(getStreamNodeId().toString())
      .withChildren(new Explanation[]{
        incomingStream.toExplanation(factory)
      })
      .withFunctionName(factory.getFunctionName(this.getClass()))
      .withImplementingClass(this.getClass().getName())
      .withExpressionType(ExpressionType.STREAM_DECORATOR)
      .withExpression(toExpression(factory, false).toString());
  }

  /**
   * Read and return the next tuple.
   * For each tuple, add a field made up of the concatenation of two fields
   */
  public Tuple read() throws IOException {

    Tuple tuple = incomingStream.read();
    if(!tuple.EOF){
      if(tuple.fields.containsKey(leftFieldName) && tuple.fields.containsKey(rightFieldName)){
        tuple.fields.put("newField", tuple.get(leftFieldName).toString() + tuple.get(rightFieldName).toString());
      }
    }

    return tuple;
  }
}