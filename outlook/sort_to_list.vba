Sub SortRecipientsByLastName_To_And_Cc()
    Dim Mail As MailItem
    Set Mail = Application.ActiveInspector.CurrentItem

    If Mail.Class = olMail Then
        ' Sort To field
        If Mail.Recipients.Count > 0 Then
            Call SortRecipientTypeByLastName(Mail, olTo)
            Call SortRecipientTypeByLastName(Mail, olCC)
        End If
    End If
End Sub

Sub SortRecipientTypeByLastName(ByRef Mail As MailItem, ByVal RecipType As Long)
    Dim Recipients As Outlook.Recipients
    Dim TempRecipients As Collection
    Dim i As Long
    Dim displayNames() As String
    Dim emailAddresses() As String
    Dim count As Long
    Dim sortedList As String

    Set Recipients = Mail.Recipients
    Set TempRecipients = New Collection

    ' Count recipients of the given type (To or Cc)
    count = 0
    For i = 1 To Recipients.Count
        If Recipients(i).Type = RecipType Then
            count = count + 1
        End If
    Next i

    If count <= 1 Then Exit Sub ' No need to sort

    ReDim displayNames(count - 1)
    ReDim emailAddresses(count - 1)

    ' Collect names and emails of given type
    count = 0
    For i = 1 To Recipients.Count
        If Recipients(i).Type = RecipType Then
            displayNames(count) = Recipients(i).Name
            emailAddresses(count) = Recipients(i).Address
            count = count + 1
        End If
    Next i

    ' Sort by last name
    Call SortByLastName(displayNames, emailAddresses)

    ' Build sorted string
    sortedList = ""
    For i = 0 To UBound(emailAddresses)
        sortedList = sortedList & emailAddresses(i)
        If i < UBound(emailAddresses) Then
            sortedList = sortedList & "; "
        End If
    Next i

    ' Apply back to Mail item
    If RecipType = olTo Then
        Mail.To = sortedList
    ElseIf RecipType = olCC Then
        Mail.CC = sortedList
    End If
End Sub

Sub SortByLastName(ByRef names() As String, ByRef emails() As String)
    Dim i As Long, j As Long
    Dim tempName As String, tempEmail As String
    For i = LBound(names) To UBound(names) - 1
        For j = i + 1 To UBound(names)
            If GetLastName(names(i)) > GetLastName(names(j)) Then
                ' Swap names
                tempName = names(i)
                names(i) = names(j)
                names(j) = tempName
                ' Swap emails to keep alignment
                tempEmail = emails(i)
                emails(i) = emails(j)
                emails(j) = tempEmail
            End If
        Next j
    Next i
End Sub

Function GetLastName(fullName As String) As String
    Dim parts() As String
    If InStr(fullName, ",") > 0 Then
        ' Format: Last, First
        parts = Split(fullName, ",")
        GetLastName = Trim(parts(0))
    Else
        ' Format: First Last
        parts = Split(fullName, " ")
        If UBound(parts) >= 1 Then
            GetLastName = Trim(parts(UBound(parts)))
        Else
            GetLastName = fullName
        End If
    End If
End Function
