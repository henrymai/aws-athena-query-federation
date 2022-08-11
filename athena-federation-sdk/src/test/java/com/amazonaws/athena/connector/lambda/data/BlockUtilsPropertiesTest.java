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
import static org.assertj.core.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.io.File;
import java.nio.file.Paths;
import java.nio.file.Files;

import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.reader.BaseReader.RepeatedListReader;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.ipc.JsonFileWriter;

import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;

class BlockUtilsPropertiesTest {

  // TODO: We could probably use an ArrowTypeVisitor for this instead
  Arbitrary<FieldVector> arbitraryForField(Field field, int numElems) {
      switch (field.getType().getTypeID()) {
          case Utf8:
              return varCharVector(field, numElems);
          case List:
              return listVector(field, numElems);
          // TODO: Add the rest of the Arbitraries for each type
          default:
              throw new RuntimeException("Not yet implemented");
      }
  }

  @Provide
  Arbitrary<FieldVector> varCharVector(Field field, int numElems) {
      assertThat(field.getType()).isEqualTo(ArrowType.Utf8.INSTANCE);
      assertThat(field.getChildren().size()).isEqualTo(0);
      return Arbitraries.strings().withCharRange('a','z').ofMinLength(1).ofMaxLength(10).list().ofSize(numElems).map(elems -> {
          VarCharVector vec = (VarCharVector) field.createVector(new RootAllocator());
          vec.allocateNew(numElems);
          for (int i = 0; i < numElems; ++i) {
              vec.setSafe(i, elems.get(i).getBytes(StandardCharsets.UTF_8));
          }
          vec.setValueCount(numElems);
          return (FieldVector) vec;
      });
  }

  @Provide
  Arbitrary<FieldVector> listVector(Field field, int numElems) {
      assertThat(field.getType()).isEqualTo(ArrowType.List.INSTANCE);
      assertThat(field.getChildren().size()).isEqualTo(1);
      //System.out.println("INSIDE LISTVECTOR: " + field);
      Field childField = field.getChildren().get(0);
      // TODO: Figure out how to properly recurse here.
      // I think the last call to arbitrary just replaces all the other calls.
      Arbitrary<FieldVector> childArb = arbitraryForField(childField, numElems);
      return childArb.map(childVec -> {
          ListVector vec = (ListVector) field.createVector(new RootAllocator());
          RepeatedListReader childReader = (RepeatedListReader) childVec.getReader();

          UnionListWriter writer = vec.getWriter();
          writer.allocate();
          writer.startList();
          int readerPos = childReader.getPosition();
          //System.out.println("READER POS: " + readerPos);
          for (int i = 0; i < numElems; ++i) {
              childReader.setPosition(readerPos++);
              childReader.copyAsValue(writer);
          }
          writer.endList();
          writer.setValueCount(numElems);
          //System.out.println("CHILD VEC:" + childVec);
          //System.out.println("INSIDE LIST VEC: " + vec);
          return vec;
      });
  }

  @Provide
  Arbitrary<ArrowType> arrowType() {
      // TODO: Add the rest of the types we support
      return Arbitraries.of(ArrowType.List.INSTANCE, ArrowType.Utf8.INSTANCE);
  }

  @Provide
  Arbitrary<FieldType> fieldType() {
      Arbitrary<ArrowType> arrowType = arrowType();
     // TODO: this might need to be false for Map keys so do a .map on the arrowType to make a new
     // Arbitrary for this.
     // I'm just keeping this simple for now to demo how to do this for List and String
      boolean nullable = true;
      return arrowType.map(atype -> new FieldType(nullable, atype, null));
  }

  @Provide
  Arbitrary<java.util.List<Field>> fieldChildren(FieldType parentType) {
      if (parentType.getType().equals(ArrowType.List.INSTANCE)) {
          Arbitrary<Field> field = Arbitraries.lazyOf(this::field);
          return field.map(f -> java.util.List.of(f));
      }
      return Arbitraries.just((java.util.List<Field>) null);
  }

  @Provide
  Arbitrary<Field> field() {
      // Combinators don't actually work for this situation.
      //
      // Meaning, I already tried passing in an Arbitrary<FieldType> parentType to fieldChildren()
      // and using a combinator across fieldName, fieldType, and fieldChildren and the generated fieldType
      // is different for the fieldChildren Arbitrary vs the newly combined Arbitrary.
      // Which would result in things like Utf8<List>, which is wrong.
      //
      // Using flatMap instead actually works consistently as you would expect.
      Arbitrary<String> fieldName = Arbitraries.strings().withCharRange('a','z').ofMinLength(1).ofMaxLength(10);
      return
          fieldName.flatMap(fN ->
              fieldType().flatMap(fT ->
                  fieldChildren(fT).map(c ->
                      new Field(fN, fT, c)
                  )
              )
          );
  }

  @Provide
  Arbitrary<VectorSchemaRoot> vectorSchemaRoot(@ForAll("field") Field field) {
      System.out.println("###############################");
      System.out.println("Field: " + field);
      return
          Arbitraries.integers().between(5, 10).flatMap(numRows ->
              arbitraryForField(field, numRows).map(fvs -> {
                  System.out.println("Field Vectors:\n" + fvs);
                  //return new VectorSchemaRoot(new Schema(java.util.List.of(field)), java.util.List.of(fvs), 1);
                  return new VectorSchemaRoot(java.util.List.of(fvs));
              })
          );
  }

  void printAsJson(VectorSchemaRoot schemaRoot) throws java.io.IOException {
      JsonFileWriter.JSONWriteConfig config = JsonFileWriter.config();
      config = config.pretty(true);

      JsonFileWriter jsonfilewriter = new JsonFileWriter(new File("/tmp/TEST.json"), config);
      jsonfilewriter.start(schemaRoot.getSchema(), null);
      jsonfilewriter.write(schemaRoot);
      jsonfilewriter.close();

      String str = new String(Files.readAllBytes(Paths.get("/tmp/TEST.json")), StandardCharsets.UTF_8);
      System.out.println(str);
  }

  @Property
  boolean setComplexValuesSetsAllFieldsCorrectlyGivenAnyInput(@ForAll("vectorSchemaRoot") VectorSchemaRoot inputSchemaRoot) throws java.io.IOException {
      System.out.println("Table:\n" + inputSchemaRoot.contentToTSVString());
      System.out.println("__________________");

      printAsJson(inputSchemaRoot);

      System.out.println("*****************************");
      return true;

      //// Create an empty `outputSchemaRoot` so that we can pass in its internal FieldVector to be written to by setComplexValue.
      //VectorSchemaRoot outputSchemaRoot = VectorSchemaRoot.create(inputSchemaRoot.getSchema(), new RootAllocator());
      //BlockUtils.setComplexValue(outputSchemaRoot.getVector(0), 0 /*I think*/, new ArrowToArrowResolver(), inputSchemaRoot.getVector(0));
      //return outputSchemaRoot.equals(inputSchemaRoot);
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
