/*************************************************************************************************
 * Status interface
 *
 * Copyright 2020 Google LLC
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *     https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 *************************************************************************************************/

package tkrzw;

/**
 * Status of operations
 */
public class Status {
  /**
   * Enumeration of status codes.
   */
  public enum Code {
    /** Success. */
    SUCCESS(0),
    /** Generic error whose cause is unknown. */
    UNKNOWN_ERROR(1),
    /** Generic error from underlying systems. */
    SYSTEM_ERROR(2),
    /** Error that the feature is not implemented. */
    NOT_IMPLEMENTED_ERROR(3),
    /** Error that a precondition is not met. */
    PRECONDITION_ERROR(4),
    /** Error that a given argument is invalid. */
    INVALID_ARGUMENT_ERROR(5),
    /** Error that the operation is canceled. */
    CANCELED_ERROR(6),
    /** Error that a specific resource is not found. */
    NOT_FOUND_ERROR(7),
    /** Error that the operation is not permitted. */
    PERMISSION_ERROR(8),
    /** Error that the operation is infeasible. */
    INFEASIBLE_ERROR(9),
    /** Error that a specific resource is duplicated. */
    DUPLICATION_ERROR(10),
    /** Error that internal data are broken. */
    BROKEN_DATA_ERROR(11),
    /** Error caused by networking failure. */
    NETWORK_ERROR(12),
    /** Generic error caused by the application logic. */
    APPLICATION_ERROR(13);

    /**
     * Returns the code enum matching an integer.
     * @param num The integer for the code.
     * @return The mathing code.
     */
    public static Code valueOf(int num) {
      switch (num) {
        case 0: return SUCCESS;
        case 1: return UNKNOWN_ERROR;
        case 2: return SYSTEM_ERROR;
        case 3: return NOT_IMPLEMENTED_ERROR;
        case 4: return PRECONDITION_ERROR;
        case 5: return INVALID_ARGUMENT_ERROR;
        case 6: return CANCELED_ERROR;
        case 7: return NOT_FOUND_ERROR;
        case 8: return PERMISSION_ERROR;
        case 9: return INFEASIBLE_ERROR;
        case 10: return DUPLICATION_ERROR;
        case 11: return BROKEN_DATA_ERROR;
        case 12: return NETWORK_ERROR;
        case 13: return APPLICATION_ERROR;
      }
      return UNKNOWN_ERROR;
    }

    /**
     * Constructor to set the ID value.
     */
    private Code(int id) {
      id_ = id;
    }

    /** The ID value. */
    private int id_;
  }

  /** Status code: Success. */
  public static final Code SUCCESS = Code.SUCCESS;
  /** Status code: Generic error whose cause is unknown. */
  public static final Code UNKNOWN_ERROR = Code.UNKNOWN_ERROR;
  /** Status code: Generic error from underlying systems. */
  public static final Code SYSTEM_ERROR = Code.SYSTEM_ERROR;
  /** Status code: Error that the feature is not implemented. */
  public static final Code NOT_IMPLEMENTED_ERROR = Code.NOT_IMPLEMENTED_ERROR;
  /** Status code: Error that a precondition is not met. */
  public static final Code PRECONDITION_ERROR = Code.PRECONDITION_ERROR;
  /** Status code: Error that a given argument is invalid. */
  public static final Code INVALID_ARGUMENT_ERROR = Code.INVALID_ARGUMENT_ERROR;
  /** Status code: Error that the operation is canceled. */
  public static final Code CANCELED_ERROR = Code.CANCELED_ERROR;
  /** Status code: Error that a specific resource is not found. */
  public static final Code NOT_FOUND_ERROR = Code.NOT_FOUND_ERROR;
  /** Status code: Error that the operation is not permitted. */
  public static final Code PERMISSION_ERROR = Code.PERMISSION_ERROR;
  /** Status code: Error that the operation is infeasible. */
  public static final Code INFEASIBLE_ERROR = Code.INFEASIBLE_ERROR;
  /** Status code: Error that a specific resource is duplicated. */
  public static final Code DUPLICATION_ERROR = Code.DUPLICATION_ERROR;
  /** Status code: Error that internal data are broken. */
  public static final Code BROKEN_DATA_ERROR = Code.BROKEN_DATA_ERROR;
  /** Status code: Error caused by networking failure. */
  public static final Code NETWORK_ERROR = Code.NETWORK_ERROR;
  /** Status code: Generic error caused by the application logic. */
  public static final Code APPLICATION_ERROR = Code.APPLICATION_ERROR;

  /**
   * Constructor representing the success code.
   */
  public Status() {
    code_ = SUCCESS;
    message_ = "";
  }

  /**
   * Constructor representing a specific status.
   * @param code The status code.
   * @param message An arbitrary status message.
   */
  public Status(Code code, String message) {
    code_ = code;
    message_ = message;
  }

  /**
   * Gets the status code.
   * @return The status code.
   */
  public Code getCode() {
    return code_;
  }

  /**
   * Gets the status message.
   * @return The status message.
   */
  public String getMessage() {
    return message_;
  }

  /**
   * Sets the code and the message.
   * @param code The status code.
   * @param message An arbitrary status message.
   */
  public void set(Code code, String message) {
    code_ = code;
    message_ = message;
  }

  /**
   * Assigns the internal state from another status object only if the current state is success.
   * @param rhs The status object.
   */
  public void join(Status rhs) {
    if (code_ == SUCCESS) {
      code_ = rhs.code_;
      message_ = rhs.message_;
    }
  }

  /**
   * Gets the string expression.
   * @return The string expression.
   */
  public String toString() {
    if (message_.isEmpty()) {
      return code_.name();
    }
    return code_.name() + ": " + message_;
  }

  /**
   * Checks equality.
   * @param rhs a status object.
   * @return True for the both operands are equal, or false if not.
   */
  public boolean equals(Status rhs) {
    return rhs.code_ == code_;
  }

  /**
   * Checks equality.
   * @param rhs a status code.
   * @return True for the both operands are equal, or false if not.
   */
  public boolean equals(Code rhs) {
    return rhs == code_;
  }

  /**
   * Clones the object.
   * @return The clone object.
   */
  public Status clone() {
    return new Status(code_, message_);
  }

  /**
   * Returns true if the status is success.
   * @return True if the status is success, or false if not.
   */
  public boolean isOK() {
    return code_ == SUCCESS;
  }

  /**
   * Throws an exception if the status is not success.
   * @throws StatusException The exception object containing the status.
   */
  public void orDie() {
    if (code_ != SUCCESS) {
      throw new StatusException(this);
    }
  }

  /**
   * Container of a status and another arbitrary object.
   */
  public static class And<T> {
    /** The status object. */
    public Status status;
    /** The value object. */
    public T value;
  }

  /** The status code. */
  private Code code_;
  /** The status message. */
  private String message_;
}

// END OF FILE
