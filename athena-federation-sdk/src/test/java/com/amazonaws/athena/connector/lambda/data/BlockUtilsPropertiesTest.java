package com.amazonaws.athena.connector.lambda.data;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2022 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import net.jqwik.api.*;
import org.assertj.core.api.*;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.memory.RootAllocator;

import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;

class BlockUtilsPropertiesTest {

  // Do not actually implement/generate this class (`FieldAndData`).
  //
  // Just using this to kind of organize the thought process.
  //
  // Also read `setComplexValuesSetsAllFieldsCorrectlyGivenAnyInput` first to get an idea of
  // what we want to generate and what the test is checking.
  //
  // ***
  // The main idea is that we have an overarching `Arbitrary` for `VectorSchemaRoot`; lets refer to
  // this as `VectorSchemaRootArbitrary`.
  // Inside `VectorSchemaRootArbitrary`, it will use an `Arbitrary` for Field (that you implement) to generate a `Field`.
  // Then given that generated `Field`, the `VectorSchemaRootArbitrary` uses `Arbitrary`s for `___Vector`s that correspond to
  // the Field's children Fields (this will have to be depth first recursive).
  // ***
  class FieldAndData {
      // Two options here:
      //
      // A)
      // Write an `Arbitrary` that generates a glue string type recursively (which means
      // you'll need `Arbitrary` generators for the primitive glue type strings first).
      // Then just use GlueFieldLexer.lex to convert that into an Arrow Field.
      // Note that we will have to eventually also test GlueFieldLexer seperately, but for now
      // its ok to pretend that we can rely on it.
      //
      // public String glueSchema;
      //
      // or
      //
      // B)
      // Write an `Arbitrary` to generate the Field directly and recursively.
      // You will need to write arbitraries for all the different kinds of `ArrowType`s that a FieldType can encapsulate.
      // See this for reference on how to construct a Field directly:
      //    https://arrow.apache.org/docs/java/quickstartguide.html#create-a-field
      public Field field;

      // Schema should just single item list containing the field generated above, like: `new Schema(List.of(field))`
      // Note that setComplexValue only operates on a single FieldVector, so we'll always only have a single Field in the Schema.
      public Schema schema;

      // Using that Field schema above, generate an arrow object (VectorSchemaRoot)
      // The inner FieldVector is what will be passed into setComplexValue as `Object value`.
      // See this for how to set the values in it:
      //    https://arrow.apache.org/docs/java/quickstartguide.html#create-a-vectorschemaroot
      public VectorSchemaRoot schemaRoot;

      // For the `FieldResolver resolver` parameter to `setComplexValue`:
      // Need to write a custom FieldResolver that just extracts Arrow values.
      // The idea is that a FieldResolver going from Arrow to Arrow should be simple and
      // that we aren't testing the FieldResolver, but rather if setComplexValue will
      // set the values correctly in for sorts of complicated glueSchemas and example values
      // generated off of that schema.

      // See this documentation from jqwik on how to build more complicated arbitraries from simpler ones here:
      //    https://jqwik.net/docs/current/user-guide.html#combining-arbitraries
      // This section on recursive arbitraries should also be useful:
      //    https://jqwik.net/docs/current/user-guide.html#recursive-arbitraries
  }

  // setComplexValue(FieldVector vector, int pos, FieldResolver resolver, Object value)
  @Property
  boolean setComplexValuesSetsAllFieldsCorrectlyGivenAnyInput(@ForAll VectorSchemaRoot inputSchemaRoot) {
      // Create an empty `outputSchemaRoot` so that we can pass in its internal FieldVector to be written to by setComplexValue.
      VectorSchemaRoot outputSchemaRoot = VectorSchemaRoot.create(inputSchemaRoot.getSchema(), new RootAllocator());
      BlockUtils.setComplexValue(outputSchemaRoot.getVector(0), 0 /*I think*/, new ArrowToArrowResolver(), inputSchemaRoot.getVector(0));
      return outputSchemaRoot.equals(inputSchemaRoot);
  }

}


class ArrowToArrowResolver
    implements FieldResolver
{
    @Override
    public Object getFieldValue(Field field, Object originalValue) {
        // TODO: Implement
        return null;
    }
}
