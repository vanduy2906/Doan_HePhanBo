package cloudduggu.com;

import java.io.Serializable;
import java.util.List;

/**
 * java bean class use to create, NLP training dataset into java object
 *
 * @author  Sarvesh Kumar (CloudDuggu.com)
 * @version 1.0
 * @since   2020-08-12
 */
@SuppressWarnings("serial")
public class TrainingDocument implements Serializable {


  private List<String> stringList;
  private String type;
  private String text;

  public List<String> getStringList() {
    return stringList;
  }

  public void setStringList(List<String> stringList) {
    this.stringList = stringList;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

}
